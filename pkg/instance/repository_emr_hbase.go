package instance

import (
	"fmt"
	"strconv"

	"github.com/tencentyun/tencentcloud-exporter/pkg/common"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	sdk "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/emr/v20190103"
	"github.com/tencentyun/tencentcloud-exporter/pkg/client"
	"github.com/tencentyun/tencentcloud-exporter/pkg/config"
)

func init() {
	registerRepository("QCE/TXMR_HBASE", NewEmrHBaseInstanceRepository)
}

type EmrHBaseInstanceRepository struct {
	credential common.CredentialIface
	client     *sdk.Client
	logger     log.Logger
}

func (repo *EmrHBaseInstanceRepository) GetInstanceKey() string {
	return "target"
}

func (repo *EmrHBaseInstanceRepository) Get(id string) (instance TcInstance, err error) {
	req := sdk.NewDescribeInstancesRequest()
	req.InstanceIds = []*string{&id}
	resp, err := repo.client.DescribeInstances(req)
	if err != nil {
		return
	}
	if len(resp.Response.ClusterList) != 1 {
		return nil, fmt.Errorf("Response instanceDetails size != 1, id=%s ", id)
	}
	meta := resp.Response.ClusterList[0]
	instance, err = NewEmrTcInstance(id, meta)
	if err != nil {
		return
	}
	return
}

func (repo *EmrHBaseInstanceRepository) ListByIds(id []string) (instances []TcInstance, err error) {
	return
}

func (repo *EmrHBaseInstanceRepository) ListByFilters(filters map[string]string) (instances []TcInstance, err error) {
	req := sdk.NewDescribeInstancesRequest()
	var offset uint64 = 0
	var limit uint64 = 100
	var total int64 = -1

	req.Offset = &offset
	req.Limit = &limit
	strategy := "clusterList"
	req.DisplayStrategy = &strategy

	if v, ok := filters["ProjectId"]; ok {
		tv, e := strconv.ParseInt(v, 10, 64)
		if e == nil {
			req.ProjectId = &tv
		}
	}
	if v, ok := filters["InstanceId"]; ok {
		req.InstanceIds = []*string{&v}
	}

getMoreInstances:
	resp, err := repo.client.DescribeInstances(req)
	if err != nil {
		return
	}
	if total == -1 {
		total = int64(*resp.Response.TotalCnt)
	}
	for _, meta := range resp.Response.ClusterList {
		ins, e := NewEmrTcInstance(*meta.ClusterId, meta)
		if e != nil {
			level.Error(repo.logger).Log("msg", "Create emr instance fail", "id", *meta.ClusterId)
			continue
		}
		instances = append(instances, ins)
	}
	offset += limit
	if offset < uint64(total) {
		req.Offset = &offset
		goto getMoreInstances
	}

	return
}

func NewEmrHBaseInstanceRepository(cred common.CredentialIface, c *config.TencentConfig, logger log.Logger) (repo TcInstanceRepository, err error) {
	cli, err := client.NewEmrClient(cred, c)
	if err != nil {
		return
	}
	repo = &EmrHBaseInstanceRepository{
		credential: cred,
		client:     cli,
		logger:     logger,
	}
	return
}
