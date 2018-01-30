package cluster

import (
	"github.com/coreos/etcd/client"
)

type clusterImpl struct {
	etcdClient client.Client
}

func createNewCluster() clusterImpl {
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	return &clusterImpl{
		etcdClient: c,
	}
}
