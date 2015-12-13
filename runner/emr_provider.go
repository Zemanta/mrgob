package runner

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"golang.org/x/crypto/ssh"
)

var clusterName = "eventlog-processor"

var (
	ErrClusterNotFound  = fmt.Errorf("EMR cluster not found")
	ErrInstanceNotFound = fmt.Errorf("EMR instance not found")
)

var (
	roundRobin = rand.Intn(1000)
	rrMutex    = sync.Mutex{}
)

type clusterState struct {
	updated time.Time

	summary        *emr.ClusterSummary
	coreInstances  []*emr.Instance
	masterInstance *emr.Instance
}

var (
	cacheTTL = time.Duration(2) * time.Minute
)

type EmrProvider struct {
	clusterCache     *clusterState
	clusterCacheLock sync.Mutex

	sshConfig *ssh.ClientConfig
	awsConfig *aws.Config
}

func NewEmrProvider(sshConfig *ssh.ClientConfig, awsConfig *aws.Config) *EmrProvider {
	return &EmrProvider{
		sshConfig: sshConfig,
		awsConfig: awsConfig,
	}
}

func (e *EmrProvider) getClusterState() (*clusterState, error) {
	e.clusterCacheLock.Lock()
	defer e.clusterCacheLock.Unlock()

	if e.clusterCache != nil && time.Now().Sub(e.clusterCache.updated) < cacheTTL {
		return e.clusterCache, nil
	}

	emrApi := emr.New(e.awsConfig)

	cc := &clusterState{
		updated: time.Now(),
	}

	clusters, err := emrApi.ListClusters(&emr.ListClustersInput{
		ClusterStates: []*string{
			aws.String("WAITING"),
			aws.String("RUNNING"),
		},
		CreatedBefore: aws.Time(time.Now()),
	})
	if err != nil {
		return nil, err
	}

	for _, c := range clusters.Clusters {
		if *c.Name != clusterName {
			continue
		}
		if cc.summary == nil || c.Status.Timeline.CreationDateTime.After(*cc.summary.Status.Timeline.CreationDateTime) {
			cc.summary = c
		}
	}

	instanceRes, err := emrApi.ListInstances(&emr.ListInstancesInput{
		ClusterId: cc.summary.Id,
		InstanceGroupTypes: []*string{
			aws.String("CORE"),
		},
	})
	if err != nil {
		return nil, err
	}
	cc.coreInstances = instanceRes.Instances

	instanceRes, err = emrApi.ListInstances(&emr.ListInstancesInput{
		ClusterId: cc.summary.Id,
		InstanceGroupTypes: []*string{
			aws.String("MASTER"),
		},
	})
	if err != nil {
		return nil, err
	}
	if len(instanceRes.Instances) == 0 {
		return nil, ErrInstanceNotFound
	}

	cc.masterInstance = instanceRes.Instances[0]

	e.clusterCache = cc

	return cc, nil
}

func (e *EmrProvider) GetMasterHost() (master string, err error) {
	cs, err := e.getClusterState()
	if err != nil {
		return "", err
	}

	return *cs.masterInstance.PublicDnsName, nil
}

func (e *EmrProvider) runon() (runon string, err error) {
	cs, err := e.getClusterState()
	if err != nil {
		return "", err
	}

	rrMutex.Lock()
	defer rrMutex.Unlock()

	for i := 0; i < len(cs.coreInstances); i++ {
		idx := roundRobin % len(cs.coreInstances)
		roundRobin = idx + 1

		inst := cs.coreInstances[idx]
		if *inst.Status.State == "RUNNING" {
			runon = *inst.PublicDnsName
			break
		}
	}

	if runon == "" {
		return "", ErrInstanceNotFound
	}

	return runon, nil
}

func (e *EmrProvider) GetNextSSHClient() (*ssh.Client, error) {
	host, err := e.runon()
	if err != nil {
		return nil, err
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", host), e.sshConfig)
	if err != nil {
		return nil, err
	}
	return client, err
}

func (e *EmrProvider) GetMasterSSHClient() (*ssh.Client, error) {
	host, err := e.GetMasterHost()
	if err != nil {
		return nil, err
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", host), e.sshConfig)
	if err != nil {
		return nil, err
	}
	return client, err
}
