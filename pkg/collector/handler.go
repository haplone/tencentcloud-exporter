package collector

import (
	"fmt"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/config"
	"github.com/tencentyun/tencentcloud-exporter/pkg/instance"
	"github.com/tencentyun/tencentcloud-exporter/pkg/util"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/tencentyun/tencentcloud-exporter/pkg/metric"
)

var (
	handlerFactoryMap = make(map[string]func(common.CredentialIface, *TcProductCollector, log.Logger) (ProductHandler, error))
)

// 每个产品的指标处理逻辑
type ProductHandler interface {
	// 获取云监控指标namespace
	GetNamespace() string
	// 对指标元数据做检验, true=可用, false=跳过
	IsMetricMetaValid(meta *metric.TcmMeta) bool
	// 修改指标元数据
	ModifyMetricMeta(meta *metric.TcmMeta) error
	// 对指标做校验, true=可用, false=跳过
	IsMetricValid(m *metric.TcmMetric) bool
	// 修改指标
	ModifyMetric(m *metric.TcmMetric) error
	// 获取该指标下符合条件的所有实例, 并生成所有的series
	GetSeries(tcmMetric *metric.TcmMetric) (series []*metric.TcmSeries, err error)
}

// 将对应的产品handler注册到Factory中
func registerHandler(namespace string, _ bool, factory func(common.CredentialIface, *TcProductCollector, log.Logger) (ProductHandler, error)) {
	handlerFactoryMap[namespace] = factory
}

type baseProductHandler struct {
	monitorQueryKey  string
	collector        *TcProductCollector
	logger           log.Logger
	consumerGroups   map[string][]*config.KafkaConsumerGroup
	hMasters         map[string][]string
	HRegionServers   map[string][]string
	starrocksBrokers map[string][]string
	starrocksFEs     map[string][]string
	starrocksBEs     map[string][]string
}

func (h *baseProductHandler) IsMetricMetaValid(meta *metric.TcmMeta) bool {
	//h.logger.Log("msg", "metric --", "name", meta.MetricName, "dimensions", strings.Join(meta.SupportDimensions, ","))
	return true
}

func (h *baseProductHandler) ModifyMetricMeta(meta *metric.TcmMeta) error {
	return nil
}

func (h *baseProductHandler) AddDimensions(m *metric.TcmMetric, ins instance.TcInstance, query map[string]string) []map[string]string {
	result := make([]map[string]string, 0)
	if len(m.Meta.SupportDimensions) == 4 && strings.ToLower(m.Meta.SupportDimensions[0]) == strings.ToLower(KafkaConsumerGroup) {
		if cgs, has := h.consumerGroups[ins.GetInstanceId()]; has {
			for _, cg := range cgs {
				tq := make(map[string]string)
				for k, v := range query {
					tq[k] = v
				}
				tq[KafkaConsumerGroup] = cg.ConsumerGroupName
				tq["topicId"] = cg.TopicId
				tq["topicName"] = cg.TopicName
				result = append(result, tq)
			}
		}
	}
	if len(m.Meta.SupportDimensions) == 2 && strings.ToLower(m.Meta.SupportDimensions[0]) == strings.ToLower(EmrHBaseMasterHost) {
		if hosts, has := h.hMasters[ins.GetInstanceId()]; has {
			for _, host := range hosts {
				tq := make(map[string]string)
				for k, v := range query {
					tq[k] = v
				}
				tq[EmrHBaseMasterHost] = host
				result = append(result, tq)
			}
		}
	}
	if len(m.Meta.SupportDimensions) == 2 && strings.ToLower(m.Meta.SupportDimensions[0]) == strings.ToLower(EmrHbaseRegionServerHost) {
		if hosts, has := h.HRegionServers[ins.GetInstanceId()]; has {
			for _, host := range hosts {
				tq := make(map[string]string)
				for k, v := range query {
					tq[k] = v
				}
				tq[EmrHbaseRegionServerHost] = host
				tq[EmrHBaseRegionServerID] = tq[EmrHBaseInstanceIDKey]
				result = append(result, tq)
			}
		}
	}
	if len(m.Meta.SupportDimensions) == 2 && strings.ToLower(m.Meta.SupportDimensions[0]) == strings.ToLower(EmrStarrocksBrokerHost) {
		if hosts, has := h.starrocksBrokers[ins.GetInstanceId()]; has {
			for _, host := range hosts {
				tq := make(map[string]string)
				for k, v := range query {
					tq[k] = v
				}
				tq[EmrStarrocksBrokerHost] = host
				tq[EmrStarrocksBrokerID] = tq[EmrStarrocksInstanceIDKey]
				result = append(result, tq)
			}
		}
	}
	if len(m.Meta.SupportDimensions) == 2 && strings.ToLower(m.Meta.SupportDimensions[0]) == strings.ToLower(EmrStarrocksBeHost) {
		if hosts, has := h.starrocksBEs[ins.GetInstanceId()]; has {
			for _, host := range hosts {
				tq := make(map[string]string)
				for k, v := range query {
					tq[k] = v
				}
				tq[EmrStarrocksBeHost] = host
				tq[EmrStarrocksBeID] = tq[EmrStarrocksInstanceIDKey]
				result = append(result, tq)
			}
		}
	}
	if len(m.Meta.SupportDimensions) == 2 && strings.ToLower(m.Meta.SupportDimensions[0]) == strings.ToLower(EmrStarrocksFeHost) {
		if hosts, has := h.starrocksFEs[ins.GetInstanceId()]; has {
			for _, host := range hosts {
				tq := make(map[string]string)
				for k, v := range query {
					tq[k] = v
				}
				tq[EmrStarrocksFeHost] = host
				tq[EmrStarrocksFeID] = tq[EmrStarrocksInstanceIDKey]
				result = append(result, tq)
			}
		}
	}

	if len(result) == 0 {
		result = append(result, query)
	}
	return result
}

func (h *baseProductHandler) IsMetricValid(m *metric.TcmMetric) bool {
	p, err := m.Meta.GetPeriod(m.Conf.StatPeriodSeconds)
	if err != nil {
		return false
	}
	if p != m.Conf.StatPeriodSeconds {
		return false
	}
	return true
}

func (h *baseProductHandler) ModifyMetric(m *metric.TcmMetric) error {
	return nil
}

func (h *baseProductHandler) GetSeries(m *metric.TcmMetric) ([]*metric.TcmSeries, error) {
	if m.Conf.IsIncludeOnlyInstance() {
		return h.GetSeriesByOnly(m)
	}

	if m.Conf.IsIncludeAllInstance() {
		return h.GetSeriesByAll(m)
	}

	if m.Conf.IsCustomQueryDimensions() {
		return h.GetSeriesByCustom(m)
	}

	return nil, fmt.Errorf("must config all_instances or only_include_instances or custom_query_dimensions")
}

func (h *baseProductHandler) GetSeriesByOnly(m *metric.TcmMetric) ([]*metric.TcmSeries, error) {
	var slist []*metric.TcmSeries
	for _, insId := range m.Conf.OnlyIncludeInstances {
		ins, err := h.collector.InstanceRepo.Get(insId)
		if err != nil {
			level.Error(h.logger).Log("msg", "Instance not found", "id", insId)
			continue
		}
		ql := map[string]string{
			h.monitorQueryKey: ins.GetMonitorQueryKey(),
		}
		qls := h.AddDimensions(m, ins, ql)
		for _, tql := range qls {
			s, err := metric.NewTcmSeries(m, tql, ins)
			if err != nil {
				level.Error(h.logger).Log("msg", "Create metric series fail",
					"metric", m.Meta.MetricName, "instance", insId)
				continue
			}
			slist = append(slist, s)
		}
	}
	return slist, nil
}

func (h *baseProductHandler) GetSeriesByAll(m *metric.TcmMetric) ([]*metric.TcmSeries, error) {
	var slist []*metric.TcmSeries
	insList, err := h.collector.InstanceRepo.ListByFilters(m.Conf.InstanceFilters)
	if err != nil {
		return nil, err
	}
	for _, ins := range insList {
		if len(m.Conf.ExcludeInstances) != 0 && util.IsStrInList(m.Conf.ExcludeInstances, ins.GetInstanceId()) {
			continue
		}
		ql := map[string]string{
			h.monitorQueryKey: ins.GetMonitorQueryKey(),
		}
		qls := h.AddDimensions(m, ins, ql)
		for _, tql := range qls {
			s, err := metric.NewTcmSeries(m, tql, ins)
			if err != nil {
				level.Error(h.logger).Log("msg", "Create metric series fail",
					"metric", m.Meta.MetricName, "instance", ins.GetInstanceId())
				continue
			}
			slist = append(slist, s)
		}
	}
	return slist, nil
}

func (h *baseProductHandler) GetSeriesByCustom(m *metric.TcmMetric) ([]*metric.TcmSeries, error) {
	var slist []*metric.TcmSeries
	for _, ql := range m.Conf.CustomQueryDimensions {
		v, ok := ql[h.monitorQueryKey]
		if !ok {
			level.Error(h.logger).Log(
				"msg", fmt.Sprintf("not found %s in queryDimensions", h.monitorQueryKey),
				"ql", fmt.Sprintf("%v", ql))
			continue
		}
		ins, err := h.collector.InstanceRepo.Get(v)
		if err != nil {
			level.Error(h.logger).Log("msg", "Instance not found", "err", err, "id", v)
			continue
		}

		s, err := metric.NewTcmSeries(m, ql, ins)
		if err != nil {
			level.Error(h.logger).Log("msg", "Create metric series fail",
				"err", err, "metric", m.Meta.MetricName, "instance", ins.GetInstanceId())
			continue
		}
		slist = append(slist, s)
	}
	return slist, nil
}
