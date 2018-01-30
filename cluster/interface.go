package cluster

type Cluster interface{}

func NewCluster() Cluster {
	return createNewCluster()
}
