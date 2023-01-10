package instance

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	sdk "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/waf/v20180125"

	"github.com/tencentyun/tencentcloud-exporter/pkg/client"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/config"
)

func init() {
	registerRepository("QCE/WAF", NewWafTcInstanceRepository)
}

type WafTcInstanceRepository struct {
	client *sdk.Client
	logger log.Logger
}

func (repo *WafTcInstanceRepository) GetInstanceKey() string {
	return "InstanceId"
}

func (repo *WafTcInstanceRepository) Get(id string) (instance TcInstance, err error) {
	req := sdk.NewDescribeDomainsRequest()
	resp, err := repo.client.DescribeDomains(req)
	if err != nil {
		return
	}
	level.Info(repo.logger).Log("count", resp.Response.Total)
	if len(resp.Response.Domains) != 1 {
		return nil, fmt.Errorf("Response instanceDetails size != 1, id=%s ", id)
	}
	meta := resp.Response.Domains[0]
	instance, err = NewWafTcInstance(id, meta)
	if err != nil {
		return
	}
	return
}

func (repo *WafTcInstanceRepository) ListByIds(id []string) (instances []TcInstance, err error) {
	return
}

func (repo *WafTcInstanceRepository) ListByFilters(filters map[string]string) (instances []TcInstance, err error) {
	req := sdk.NewDescribeDomainsRequest()
	var offset uint64 = 1
	var limit uint64 = 100
	var total int64 = -1

	req.Offset = &offset
	req.Limit = &limit

getMoreInstances:
	resp, err := repo.client.DescribeDomains(req)
	if err != nil {
		return
	}
	if total == -1 {
		total = int64(*resp.Response.Total)
	}
	level.Info(repo.logger).Log("count", resp.Response.Total)
	level.Info(repo.logger).Log("RequestId", resp.Response.RequestId)
	for _, meta := range resp.Response.Domains {
		ins, e := NewWafTcInstance(*meta.InstanceId, meta)
		if e != nil {
			level.Error(repo.logger).Log("msg", "Create Waf instance fail", "id", *meta.InstanceId)
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

func NewWafTcInstanceRepository(cred common.CredentialIface, c *config.TencentConfig, logger log.Logger) (repo TcInstanceRepository, err error) {
	cli, err := client.NewWafClient(cred, c)
	if err != nil {
		return
	}
	repo = &WafTcInstanceRepository{
		client: cli,
		logger: logger,
	}
	return
}
