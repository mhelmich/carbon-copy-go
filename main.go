/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mhelmich/carbon-copy-go/cache"
	"github.com/mhelmich/carbon-copy-go/cluster"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote"
)

func main() {
	if len(os.Args) != 3 || os.Args[1] != "server" {
		printUsage()
	}

	configFileName := os.Args[2]
	if configFileName == "" {
		log.Panic("Config file name is empty")
	}

	cfg := loadConfigStandalone(configFileName)

	startPprofServer(cfg.pprofPort)

	cluster, err := cluster.NewCluster(cfg.cluster)
	if err != nil {
		log.Panicf("Can't start cluster: %s", err.Error())
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// register shutdown hook and call cleanup
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-c
		cleanup(sig, cluster)
		wg.Done()
	}()

	wg.Wait()
}

func startPprofServer(port int) {
	go func() {
		pprofAddr := fmt.Sprintf("localhost:%d", port)
		err := http.ListenAndServe(pprofAddr, nil)
		if err != nil {
			log.Warnf("Can't start pprof endpoint: %s", err.Error())
		}
	}()
}

type carbonGridConfig struct {
	cluster cluster.ClusterConfig
	cache   cache.CacheConfig

	pprofPort int
}

func printUsage() {
	fmt.Println("Usage: ./carbon-copy-go server <path_to_config>")
}

func loadConfigStandalone(configFileName string) carbonGridConfig {
	b, err := ioutil.ReadFile(configFileName)
	if err != nil {
		log.Panicf("Can't read config file: %s", err)
	}

	cfg := viper.New()
	cfg.SetConfigType("yaml")
	err = cfg.ReadConfig(bytes.NewBuffer(b))
	if err != nil {
		log.Panicf("Couldn't read config file %s", err)
	}

	var clusterConfig cluster.ClusterConfig
	err = cfg.UnmarshalKey("cluster", &clusterConfig)
	if err != nil {
		log.Panicf("Couldn't unmarshall struct %s", err)
	}

	return carbonGridConfig{
		cluster: clusterConfig,
	}
}

func cleanup(sig os.Signal, c cluster.Cluster) {
	log.Info("This node is going down gracefully")
	log.Infof("Received signal: %s", sig)
	c.Close()
}
