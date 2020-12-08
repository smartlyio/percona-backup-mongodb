package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
)

func status(cn *pbm.PBM) {
	printh("Cluster:")
	// clusterStatus(cn)
	c, err := clusterStatus2(cn)
	if err != nil {
		log.Println("ERROR:", err)
	} else {
		fmt.Println(c)
	}

	printh("PITR incremental backup:")
	pitrStatus(cn)

	printh("Currently running:")
	op := opsStatus(cn)
	if op != "" {
		fmt.Println(op)
	} else {
		fmt.Println("(none)")
	}

	printh("Storage (compressed size):")
	storageStatus(cn)
}

func opsStatus(cn *pbm.PBM) string {

	// check for ops
	lk, err := findLock(cn, cn.GetLocks)
	if err != nil {
		log.Println("ERROR:", err)
		return ""
	}

	if lk == nil {
		// check for delete ops
		lk, err = findLock(cn, cn.GetOpLocks)
		if err != nil {
			log.Println("ERROR:", err)
			return ""
		}
	}

	if lk == nil {
		return ""
	}

	// recheacing here means no conflict operation, hence all locks are the same,
	// hence any lock in `lk` contais info on the current op
	switch lk.Type {
	case pbm.CmdCancelBackup, pbm.CmdDeleteBackup, pbm.CmdResyncBackupList:
		return fmt.Sprintf("%s [op id: %s]", lk.Type, lk.OPID)
	case pbm.CmdBackup:
		bcp, err := cn.GetBackupByOPID(lk.OPID)
		if err != nil {
			return fmt.Sprintf("ERROR: get backup info: %v", err)
		}
		return fmt.Sprintf("Backup \"%s\", started at %s. Status: %s. [op id: %s]",
			bcp.Name, time.Unix((bcp.StartTS), 0).Format(plog.LogTimeFormat),
			bcp.Status, lk.OPID,
		)
	case pbm.CmdRestore, pbm.CmdPITRestore:
		rst, err := cn.GetRestoreMetaByOPID(lk.OPID)
		if err != nil {
			return fmt.Sprintf("ERROR: get backup info: %v", err)
		}
		return fmt.Sprintf("%s \"%s\", started at %s. Status: %s. [op id: %s]",
			lk.Type, rst.Backup, time.Unix((rst.StartTS), 0).UTC().Format("2006-01-02T15:04:05Z"),
			rst.Status, lk.OPID,
		)
	}

	return ""
}

func findLock(cn *pbm.PBM, fn func(*pbm.LockHeader) ([]pbm.LockData, error)) (*pbm.LockData, error) {
	locks, err := fn(&pbm.LockHeader{})
	if err != nil {
		return nil, errors.Wrap(err, "get locks")
	}

	ct, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster time")
	}

	var lk *pbm.LockData
	for _, l := range locks {
		// We don't care about the PITR slicing here. It is a subject of other status sections
		if l.Type == pbm.CmdPITR || l.Heartbeat.T+pbm.StaleFrameSec < ct.T {
			continue
		}

		// Just check if all locks are for the same op
		//
		// It could happen that the healthy `lk` became stale by the time this check
		// or the op was finished and the new one was started. So the `l.Type != lk.Type`
		// would be true but for the legit reason (no error).
		// But chances for that are quite low and on the next run of `pbm status` everythin
		//  would be ok. So no reason to complicate code to avoid that.
		if lk != nil && l.OPID != lk.OPID {
			if err != nil {
				return nil, errors.Errorf("conflicting ops running: [%s/%s::%s-%s] [%s/%s::%s-%s]",
					l.Replset, l.Node, l.Type, l.OPID,
					lk.Replset, lk.Node, lk.Type, lk.OPID,
				)
			}
		}

		lk = &l
	}

	return lk, nil
}

func storageStatus(cn *pbm.PBM) {
	bcps, err := cn.BackupsList(0)
	if err != nil {
		log.Println("ERROR: get backups list:", err)
		return
	}

	stg, err := cn.GetStorage(nil)
	if err != nil {
		log.Println("ERROR: get storage:", err)
		return
	}

	cfg, err := cn.GetConfig()
	if err != nil {
		log.Println("ERROR: get config:", err)
		return
	}

	switch cfg.Storage.Type {
	case pbm.StorageS3:
		var url []string
		if cfg.Storage.S3.EndpointURL != "" {
			url = append(url, cfg.Storage.S3.EndpointURL)
		}
		url = append(url, cfg.Storage.S3.Bucket)
		if cfg.Storage.S3.Prefix != "" {
			url = append(url, cfg.Storage.S3.Prefix)
		}
		r := ""
		if cfg.Storage.S3.Region != "" {
			r = " " + cfg.Storage.S3.Region
		}
		fmt.Printf("S3%s %s\n", r, strings.Join(url, "/"))
	case pbm.StorageFilesystem:
		fmt.Printf("FS %s\n", cfg.Storage.Filesystem.Path)
	}

	var lts primitive.Timestamp
	for _, bcp := range bcps {
		if bcp.Status != pbm.StatusDone {
			continue
		}
		var sz, opsz int64
		for _, rs := range bcp.Replsets {
			ds, err := stg.FileStat(rs.DumpName)
			if err != nil {
				log.Printf("ERROR: get file %s: %v", rs.DumpName, err)
			}

			sz += ds.Size

			os, err := stg.FileStat(rs.OplogName)
			if err != nil {
				log.Printf("ERROR: get file %s: %v", rs.OplogName, err)
			}

			sz += os.Size

			if lts.T > 0 {
				cnhks, err := cn.PITRGetChunksSlice(rs.Name, rs.FirstWriteTS, lts)
				if err != nil {
					log.Printf("ERROR: get PITR chunks for %s/%s", bcp.Name, rs.Name)
					continue
				}
				for _, c := range cnhks {
					csz, err := stg.FileStat(c.FName)
					if err != nil {
						log.Printf("ERROR: get PITR file %s: %v", c.FName, err)
					}
					opsz += csz.Size
				}
			}
		}
		lts = bcp.LastWriteTS
		if opsz > 0 {
			fmt.Printf("    PITR chunks %s\n", fmtSize(opsz))
		}
		fmt.Printf("  %s %s [complete: %s]\n", bcp.Name, fmtSize(sz), time.Unix(int64(bcp.LastWriteTS.T), 0).UTC().Format("2006-01-02T15:04:05"))
	}
}

func fmtSize(size int64) string {
	const (
		_          = iota
		KB float64 = 1 << (10 * iota)
		MB
		GB
		TB
	)

	s := float64(size)

	switch {
	case s >= TB:
		return fmt.Sprintf("%.2fTB", s/TB)
	case s >= GB:
		return fmt.Sprintf("%.2fGB", s/GB)
	case s >= MB:
		return fmt.Sprintf("%.2fMB", s/MB)
	case s >= KB:
		return fmt.Sprintf("%.2fKB", s/KB)
	}
	return fmt.Sprintf("%.2fB", s)
}

func pitrStatus(cn *pbm.PBM) {
	on, err := cn.IsPITR()
	if err != nil {
		fmt.Println("ERROR: unable check PITR status:", err)
		return
	}
	fmt.Print("Status ")
	if !on {
		fmt.Println("[OFF]")
		return
	}
	fmt.Println("[ON]")
	epch, err := cn.GetEpoch()
	if err != nil {
		log.Printf("ERROR: get current epoch: %v", err)
		return
	}

	l, err := cn.LogGet(
		&plog.LogRequest{
			LogKeys: plog.LogKeys{
				Severity: plog.Error,
				Event:    string(pbm.CmdPITR),
				Epoch:    epch.TS(),
			},
		},
		1)
	if err != nil {
		log.Printf("ERROR: get log records: %v", err)
		return
	}
	if len(l) > 0 {
		fmt.Printf("! ERROR while running PITR backup: %s\n", &l[0])
	}
}

func printh(s string) {
	fmt.Println()
	fmt.Printf("%s\n", s)
	fmt.Println(strings.Repeat("=", len(s)))
}

type cluster []rs

type rs struct {
	Name  string `json:"rs"`
	Nodes []node `json:"nodes"`
}

type node struct {
	Host string   `json:"host"`
	Ver  string   `json:"agent"`
	OK   bool     `json:"ok"`
	Errs []string `json:"errors,omitempty"`
}

func (n node) String() (s string) {
	s += fmt.Sprintf("%s: pbm-agent %v", n.Host, n.Ver)
	if n.OK {
		s += fmt.Sprintf(" OK")
		return s
	}
	s += fmt.Sprintf(" FAILED status:")
	for _, e := range n.Errs {
		s += fmt.Sprintf("\n      > ERROR with %s", e)
	}

	return s
}

func (c cluster) String() (s string) {
	for _, rs := range c {
		s += fmt.Sprintf("%s:\n", rs.Name)
		for _, n := range rs.Nodes {
			s += fmt.Sprintf("  - %s\n", n)
		}
	}
	return s
}

func clusterStatus2(cn *pbm.PBM) (cluster, error) {
	inf, err := cn.GetNodeInfo()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster info")
	}

	type clstr struct {
		rs    string
		nodes []string
	}
	topology := []clstr{
		{
			rs:    inf.SetName,
			nodes: inf.Hosts,
		},
	}

	if inf.IsSharded() {
		shrd, err := cn.GetShards()
		if err != nil {
			return nil, errors.Wrap(err, "get cluster shards")
		}
		for _, s := range shrd {
			topology = append(topology, clstr{
				rs:    s.ID,
				nodes: strings.Split(strings.TrimLeft(s.Host, s.ID+"/"), ","),
			})
		}
	}

	err = cn.AgentStatusGC()
	if err != nil {
		return nil, errors.Wrap(err, "clean-up stale agent statuses")
	}

	var ret cluster
	for _, shrd := range topology {
		lrs := rs{Name: shrd.rs}
		for i, n := range shrd.nodes {
			lrs.Nodes = append(lrs.Nodes, node{Host: shrd.rs + "/" + n})

			nd := &lrs.Nodes[i]

			stat, err := cn.GetAgentStatus(shrd.rs, n)
			if errors.Is(err, mongo.ErrNoDocuments) {
				nd.Ver = "NOT FOUND"
				continue
			} else if err != nil {
				nd.Errs = append(nd.Errs, fmt.Sprintf("ERROR: get agent status: %v", err))
				continue
			}
			nd.Ver = "v" + stat.Ver
			nd.OK, nd.Errs = stat.OK()
		}
		ret = append(ret, lrs)
	}

	return ret, nil
}
