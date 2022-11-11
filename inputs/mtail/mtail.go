package mtail

import (
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/net/context"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/pkg/prom"
	"flashcat.cloud/categraf/types"
	"github.com/google/mtail/pkg/mtail"
)

const inputName = `mtail`
const description = ` extract internal monitoring data from application logs`

// MTail holds the configuration for the plugin.
type MTail struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

type Instance struct {
	config.InstanceConfig

	NamePrefix           string   `toml:"name_prefix"`
	Progs                string   `toml:"progs"`
	Logs                 []string `toml:"logs"`
	IgnoreFileRegPattern string   `toml:"ignore_filename_regex_pattern"`
	OverrideTimeZone     string   `toml:"override_timezone"`
	EmitProgLabel        string   `toml:"emit_prog_label"`
	EmitMetricTimestamp  string   `toml:"emit_metric_timestamp"`

	PollInterval       time.Duration `toml:"poll_interval"`
	PollLogInterval    time.Duration `toml:"poll_log_interval"`
	MetricPushInterval time.Duration `toml:"metric_push_interval"`
	MaxRegexpLen       int           `toml:"max_regexp_length"`
	MaxRecursionDepth  int           `toml:"max_recursion_depth"`

	SyslogUseCurrentYear string `toml:"syslog_use_current_year"` // true
	sysLogUseCurrentYear bool   `toml:"-"`
	LogRuntimeErrors     string `toml:"vm_logs_runtime_errors"` // true
	logRuntimeErrors     bool   `toml:"-"`
	//
	ctx    context.Context    `toml:"-"`
	cancel context.CancelFunc `toml:"-"`
	m      *prometheus.Registry
}

func (ins *Instance) Init() error {

	if len(ins.Progs) == 0 || len(ins.Logs) == 0 {
		return types.ErrInstancesEmpty
	}

	opt := mtail.Option{}
	opt.Logs = ins.Logs
	opt.Progs = ins.Progs
	opt.NamePrefix = ins.NamePrefix
	opt.IgnoreFileRegPattern = ins.IgnoreFileRegPattern
	opt.OverrideTimeZone = ins.OverrideTimeZone
	opt.Version = config.Version

	// set default value
	if ins.SyslogUseCurrentYear != "false" {
		opt.SyslogUseCurrentYear = true
	}
	if ins.LogRuntimeErrors != "false" {
		opt.LogRuntimeErrors = true
	}
	if ins.EmitProgLabel != "false" {
		opt.OmitProgLabel = true
	}
	if ins.PollLogInterval == 0 {
		opt.PollLogInterval = 250 * time.Millisecond
	}
	if ins.PollInterval == 0 {
		opt.PollInterval = 250 * time.Millisecond
	}
	if ins.MetricPushInterval == 0 {
		opt.MetricPushInterval = 1 * time.Minute
	}
	if ins.MaxRegexpLen == 0 {
		opt.MaxRegexpLen = 1024
	}
	if ins.MaxRecursionDepth == 0 {
		opt.MaxRecursionDepth = 100
	}
	if ins.cancel != nil {
		ins.cancel()
	} else {
		ins.ctx, ins.cancel = context.WithCancel(context.Background())
	}
	registry, err := mtail.GetRegistry(ins.ctx, opt)
	if err != nil {
		log.Println(err)
		ins.cancel()
		return err
	}
	ins.m = registry

	return nil
}

func (ins *Instance) Drop() {
	ins.cancel()
}

func init() {
	inputs.Add(inputName, func() inputs.Input {
		return &MTail{}
	})
}

func (s *MTail) GetInstances() []inputs.Instance {
	ret := make([]inputs.Instance, len(s.Instances))
	for i := 0; i < len(s.Instances); i++ {
		ret[i] = s.Instances[i]
	}
	return ret
}

// Description returns a one-sentence description on the input.
func (s *MTail) Description() string {
	return description
}

// Gather retrieves all the configured fields and tables.
// Any error encountered does not halt the process. The errors are accumulated
// and returned at the end.
// func (s *Instance) Gather(acc telegraf.Accumulator) error {
func (ins *Instance) Gather(slist *types.SampleList) {
	reg := ins.m
	mfs, done, err := prometheus.ToTransactionalGatherer(reg).Gather()
	if err != nil {
		log.Println(err)
		return
	}
	defer done()

	for _, mf := range mfs {
		metricName := mf.GetName()
		for _, m := range mf.Metric {
			// reading tags
			tags := ins.makeLabels(m)

			if mf.GetType() == dto.MetricType_SUMMARY {
				ins.HandleSummary(m, tags, metricName, slist)
			} else if mf.GetType() == dto.MetricType_HISTOGRAM {
				ins.HandleHistogram(m, tags, metricName, slist)
			} else {
				ins.handleGaugeCounter(m, tags, metricName, slist)
			}
		}
	}
}

func (p *Instance) makeLabels(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for key, value := range p.Labels {
		result[key] = value
	}
	return result
}

func (p *Instance) HandleSummary(m *dto.Metric, tags map[string]string, metricName string, slist *types.SampleList) {
	namePrefix := ""
	if !strings.HasPrefix(metricName, p.NamePrefix) {
		namePrefix = p.NamePrefix
	}
	slist.PushFront(types.NewSample("", prom.BuildMetric(namePrefix, metricName, "count"), float64(m.GetSummary().GetSampleCount()), tags))
	slist.PushFront(types.NewSample("", prom.BuildMetric(namePrefix, metricName, "sum"), m.GetSummary().GetSampleSum(), tags))

	for _, q := range m.GetSummary().Quantile {
		slist.PushFront(types.NewSample("", prom.BuildMetric(namePrefix, metricName), q.GetValue(), tags, map[string]string{"quantile": fmt.Sprint(q.GetQuantile())}))
	}
}

func (p *Instance) HandleHistogram(m *dto.Metric, tags map[string]string, metricName string, slist *types.SampleList) {
	namePrefix := ""
	if !strings.HasPrefix(metricName, p.NamePrefix) {
		namePrefix = p.NamePrefix
	}
	slist.PushFront(types.NewSample("", prom.BuildMetric(namePrefix, metricName, "count"), float64(m.GetHistogram().GetSampleCount()), tags))
	slist.PushFront(types.NewSample("", prom.BuildMetric(namePrefix, metricName, "sum"), m.GetHistogram().GetSampleSum(), tags))
	slist.PushFront(types.NewSample("", prom.BuildMetric(namePrefix, metricName, "bucket"), float64(m.GetHistogram().GetSampleCount()), tags, map[string]string{"le": "+Inf"}))

	for _, b := range m.GetHistogram().Bucket {
		le := fmt.Sprint(b.GetUpperBound())
		value := float64(b.GetCumulativeCount())
		slist.PushFront(types.NewSample("", prom.BuildMetric(namePrefix, metricName, "bucket"), value, tags, map[string]string{"le": le}))
	}
}

func (p *Instance) handleGaugeCounter(m *dto.Metric, tags map[string]string, metricName string, slist *types.SampleList) {
	fields := getNameAndValue(m, metricName)
	for metric, value := range fields {
		if !strings.HasPrefix(metric, p.NamePrefix) {
			slist.PushFront(types.NewSample("", prom.BuildMetric(p.NamePrefix, metric, ""), value, tags))
		} else {
			slist.PushFront(types.NewSample("", prom.BuildMetric("", metric, ""), value, tags))
		}

	}
}

func getNameAndValue(m *dto.Metric, metricName string) map[string]interface{} {
	fields := make(map[string]interface{})
	if m.Gauge != nil {
		if !math.IsNaN(m.GetGauge().GetValue()) {
			fields[metricName] = m.GetGauge().GetValue()
		}
	} else if m.Counter != nil {
		if !math.IsNaN(m.GetCounter().GetValue()) {
			fields[metricName] = m.GetCounter().GetValue()
		}
	} else if m.Untyped != nil {
		if !math.IsNaN(m.GetUntyped().GetValue()) {
			fields[metricName] = m.GetUntyped().GetValue()
		}
	}
	return fields
}
