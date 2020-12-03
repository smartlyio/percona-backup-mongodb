package main

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func status(cn *pbm.PBM) {
	inf, err := cn.GetNodeInfo()
	if err != nil {
		log.Fatalln("ERROR: get cluster info:", err)
	}

	type rs struct {
		rs    string
		nodes []string
	}
	topology := []rs{
		{
			rs:    inf.SetName,
			nodes: inf.Hosts,
		},
	}

	printh("Database")
	if inf.IsSharded() {
		shrd, err := cn.GetShards()
		if err != nil {
			log.Fatalln("ERROR: get cluster shards:", err)
		}
		fmt.Printf("%d-shard cluster with current primary configsvr %s\n", len(shrd), inf.Primary)
		for _, s := range shrd {
			topology = append(topology, rs{
				rs:    s.ID,
				nodes: strings.Split(strings.TrimLeft(s.Host, s.ID+"/"), ","),
			})
		}
	} else {
		fmt.Printf("%d-node non-sharded replicaset, current primary %s\n", len(inf.Hosts), inf.Primary)
	}

	err = cn.AgentStatusGC()
	if err != nil {
		log.Fatalln("ERROR: clean-up stale agent statuses:", err)
	}

	printh("Agents")
	for _, shrd := range topology {
		fmt.Printf("%s:\n", shrd.rs)
		for _, node := range shrd.nodes {
			stat, err := cn.GetAgentStatus(shrd.rs, node)
			if errors.Is(err, mongo.ErrNoDocuments) {
				fmt.Printf("  - %s/%s: pbm-agent NOT FOUND\n", shrd.rs, node)
				continue
			} else if err != nil {
				log.Printf("ERROR: get status for %s/%s agent: %v\n", shrd.rs, node, err)
				continue
			}

			fstat := ""
			if ok, errs := stat.OK(); !ok {
				fstat = "FAILED status:\n      > ERROR with " + strings.Join(errs, "\n      > ERROR with ")
			} else {
				fstat = "OK"
			}
			fmt.Printf("  - %s: pbm-agent v%s %s\n", node, stat.Ver, fstat)
		}
	}
}

func getAgentStatus() {}

func printh(s string) {
	fmt.Println()
	fmt.Printf("%s:\n", s)
	fmt.Println(strings.Repeat("=", len(s)+1))
}
