package collector

import (
	"github.com/go-kit/log"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/config"
	"github.com/tencentyun/tencentcloud-exporter/pkg/metric"
	"strings"
)

const (
	KafkaNamespace     = "QCE/CKAFKA"
	KafkaInstanceIDKey = "instanceId"
	KafkaConsumerGroup = "consumerGroup"
)

func init() {
	registerHandler(KafkaNamespace, defaultHandlerEnabled, NewKafkaHandler)
}

type kafkaHandler struct {
	baseProductHandler
}

func (h *kafkaHandler) GetNamespace() string {
	return MariaDBNamespace
}

func (h *kafkaHandler) IsMetricValid(m *metric.TcmMetric) bool {
	h.logger.Log("msg", "metric ==", "name", m.Meta.MetricName, "dimensions", strings.Join(m.Meta.SupportDimensions, ","))
	if len(m.Meta.SupportDimensions) != 1 && len(m.Meta.SupportDimensions) != 4 {
		return false
	}
	dimensionName := strings.ToLower(m.Meta.SupportDimensions[0])
	if dimensionName != strings.ToLower(KafkaInstanceIDKey) &&
		dimensionName != strings.ToLower(KafkaConsumerGroup) {
		return false
	}
	p, err := m.Meta.GetPeriod(m.Conf.StatPeriodSeconds)
	if err != nil {
		return false
	}
	if p != m.Conf.StatPeriodSeconds {
		return false
	}
	return true
}

func NewKafkaHandler(cred common.CredentialIface, c *TcProductCollector, logger log.Logger) (ProductHandler, error) {
	handler := &kafkaHandler{
		baseProductHandler{
			monitorQueryKey: KafkaInstanceIDKey,
			collector:       c,
			logger:          logger,
			consumerGroups:  make(map[string][]*config.KafkaConsumerGroup),
		},
	}

	for _, value := range c.ProductConf.KafkaConsumerGroups {
		cg, err := config.NewKafkaConsumerGroup(value)
		if err != nil {
			logger.Log("msg", "错误的kafka消费组配置", "err", err.Error())
		} else {
			if list, has := handler.consumerGroups[cg.InstanceId]; has {
				list = append(list, cg)
				handler.consumerGroups[cg.InstanceId] = list
			} else {
				var tl []*config.KafkaConsumerGroup
				tl = append(tl, cg)
				handler.consumerGroups[cg.InstanceId] = tl
			}
		}
	}
	return handler, nil

}
