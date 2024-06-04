package basic

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/percona/rds_exporter/config"
	"github.com/percona/rds_exporter/sessions"
)

const (
	// GBtoByte is a constant that Gigabyte values can be multiplied with to get Bytes.
	GBtoByte = 1e9
)

var (
	Period = 60 * time.Second
	Delay  = 600 * time.Second
	Range  = 600 * time.Second
)

type Scraper struct {
	// params
	instance        *config.Instance
	sessionInstance sessions.Instance
	collector       *Collector
	ch              chan<- prometheus.Metric

	// internal
	svc         *cloudwatch.CloudWatch
	constLabels prometheus.Labels
}

func NewScraper(instance *config.Instance, collector *Collector, ch chan<- prometheus.Metric) *Scraper {
	// Create CloudWatch client
	sess, sessInstance := collector.sessions.GetSession(instance.Region, instance.Instance)
	if sess == nil {
		return nil
	}
	svc := cloudwatch.New(sess)

	constLabels := prometheus.Labels{
		"region":   instance.Region,
		"instance": instance.Instance,
	}
	for n, v := range instance.Labels {
		if v == "" {
			delete(constLabels, n)
		} else {
			constLabels[n] = v
		}
	}

	return &Scraper{
		// params
		instance:        instance,
		sessionInstance: *sessInstance,
		collector:       collector,
		ch:              ch,

		// internal
		svc:         svc,
		constLabels: constLabels,
	}
}

func getLatestDatapoint(datapoints []*cloudwatch.Datapoint) *cloudwatch.Datapoint {
	var latest *cloudwatch.Datapoint

	for dp := range datapoints {
		if latest == nil || latest.Timestamp.Before(*datapoints[dp].Timestamp) {
			latest = datapoints[dp]
		}
	}

	return latest
}

// Scrape makes the required calls to AWS CloudWatch by using the parameters in the Collector.
// Once converted into Prometheus format, the metrics are pushed on the ch channel.
func (s *Scraper) Scrape() {
	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(len(s.collector.metrics))

	for _, metric := range s.collector.metrics {
		metric := metric
		go func() {
			defer wg.Done()

			if err := s.scrapeMetricSomewhere(metric); err != nil {
				level.Error(s.collector.l).Log("metric", metric.cwName, "error", err)
			}
			if err := s.scrapeMetricFromGetMetricsStatistics(metric); err != nil {
				level.Error(s.collector.l).Log("metric", metric.cwName, "error", err)
			}
		}()
	}
}

func (s *Scraper) scrapeMetricSomewhere(metric Metric) error {
	var value float64

	switch metric.cwName {
	case "TotalStorageSpace":
		value = float64(s.sessionInstance.AllocatedStorage) * GBtoByte
	case "TotalMemory":
		var err error

		value, err = GetInstanceMaxMemory(s.sessionInstance.InstanceClass)
		if err != nil {
			return err
		}
	default:
		return nil
	}

	s.ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(metric.prometheusName, metric.prometheusHelp, nil, s.constLabels),
		prometheus.GaugeValue,
		value,
	)

	return nil
}

func (s *Scraper) scrapeMetricFromGetMetricsStatistics(metric Metric) error {
	now := time.Now()
	end := now.Add(-Delay)

	params := &cloudwatch.GetMetricStatisticsInput{
		EndTime:   aws.Time(end),
		StartTime: aws.Time(end.Add(-Range)),

		Period:     aws.Int64(int64(Period.Seconds())),
		MetricName: aws.String(metric.cwName),
		Namespace:  aws.String("AWS/RDS"),
		Dimensions: []*cloudwatch.Dimension{},
		Statistics: aws.StringSlice([]string{"Average"}),
		Unit:       nil,
	}

	params.Dimensions = append(params.Dimensions, &cloudwatch.Dimension{
		Name:  aws.String("DBInstanceIdentifier"),
		Value: aws.String(s.instance.Instance),
	})

	// Call CloudWatch to gather the datapoints
	resp, err := s.svc.GetMetricStatistics(params)
	if err != nil {
		return err
	}

	// There's nothing in there, don't publish the metric
	if len(resp.Datapoints) == 0 {
		return nil
	}

	// Pick the latest datapoint
	dp := getLatestDatapoint(resp.Datapoints)

	// Get the metric.
	v := aws.Float64Value(dp.Average)
	switch metric.cwName {
	case "EngineUptime":
		// "Fake EngineUptime -> node_boot_time with time.Now().Unix() - EngineUptime."
		v = float64(time.Now().Unix() - int64(v))
	}

	// Send metric.
	s.ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(metric.prometheusName, metric.prometheusHelp, nil, s.constLabels),
		prometheus.GaugeValue,
		v,
	)

	return nil
}
