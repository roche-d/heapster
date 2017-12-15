// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"crypto/tls"
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

const (
	brokerClientID           = "kafka-sink"
	brokerDialTimeout        = 10 * time.Second
	brokerDialRetryLimit     = 1
	brokerDialRetryWait      = 0
	brokerAllowTopicCreation = true
	brokerLeaderRetryLimit   = 1
	brokerLeaderRetryWait    = 0
	metricsTopic             = "heapster-metrics"
	eventsTopic              = "heapster-events"
)

const (
	TimeSeriesTopic = "timeseriestopic"
	EventsTopic     = "eventstopic"
	compression     = "compression"
)

type KafkaClient interface {
	Name() string
	Stop()
	ProduceKafkaMessage(msgData interface{}) error
}

type kafkaSink struct {
	producer  sarama.AsyncProducer
	dataTopic string
}

type kafkaSinkConfiguration struct {
	config  *sarama.Config
	brokers []string
	topic   string
}

func (sink *kafkaSink) ProduceKafkaMessage(msgData interface{}) error {
	start := time.Now()
	msgJson, err := json.Marshal(msgData)
	if err != nil {
		return fmt.Errorf("failed to transform the items to json : %s", err)
	}

	sink.producer.Input() <- &sarama.ProducerMessage{Topic: sink.dataTopic, Value: sarama.StringEncoder(string(msgJson))}
	if err != nil {
		return fmt.Errorf("failed to produce message to %s: %s", sink.dataTopic, err)
	}
	end := time.Now()
	glog.V(4).Infof("Exported %d data to kafka in %s", len([]byte(string(msgJson))), end.Sub(start))
	return nil
}

func (sink *kafkaSink) Name() string {
	return "Apache Kafka Sink"
}

func (sink *kafkaSink) Stop() {
	sink.producer.AsyncClose()
}

func setupProducer(sinkConf *kafkaSinkConfiguration) (sarama.AsyncProducer, error) {
	glog.V(3).Infof("attempting to setup kafka sink")

	sinkConf.config.ClientID = brokerClientID
	sinkConf.config.Net.DialTimeout = brokerDialTimeout

	sinkConf.config.Producer.RequiredAcks = sarama.WaitForLocal
	sinkConf.config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	producer, err := sarama.NewAsyncProducer(sinkConf.brokers, sinkConf.config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to kafka cluster: %s", err)
	}
	glog.V(3).Infof("kafka sink setup successfully")
	return producer, nil
}

func getTopic(opts map[string][]string, topicType string) (string, error) {
	var topic string
	switch topicType {
	case TimeSeriesTopic:
		topic = metricsTopic
	case EventsTopic:
		topic = eventsTopic
	default:
		return "", fmt.Errorf("Topic type '%s' is illegal.", topicType)
	}

	if len(opts[topicType]) > 0 {
		topic = opts[topicType][0]
	}

	return topic, nil
}

func getCompression(opts map[string][]string) (sarama.CompressionCodec, error) {
	if len(opts[compression]) == 0 {
		return sarama.CompressionNone, nil
	}
	comp := opts[compression][0]
	switch comp {
	case "none":
		return sarama.CompressionNone, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	default:
		return sarama.CompressionNone, fmt.Errorf("Compression '%s' is illegal. Use none or gzip", comp)
	}
}

func buildConfig(uri *url.URL, topicType string) (*kafkaSinkConfiguration, error) {
	opts, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url's query string: %s", err)
	}
	glog.V(3).Infof("kafka sink option: %v", opts)

	// set up base sarama configuration
	conf := sarama.NewConfig()

	var kafkaBrokers []string
	if len(opts["brokers"]) < 1 {
		return nil, fmt.Errorf("There is no broker assigned for connecting kafka")
	}
	kafkaBrokers = append(kafkaBrokers, opts["brokers"]...)
	glog.V(2).Infof("initializing kafka sink with brokers - %v", kafkaBrokers)

	topic, err := getTopic(opts, topicType)
	if err != nil {
		return nil, err
	}
	compression, err := getCompression(opts)
	if err != nil {
		return nil, err
	}
	conf.Producer.Compression = compression

	if user := opts["user"]; len(user) > 0 {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.User = user[0]
	}
	if password := opts["password"]; len(password) > 0 {
		conf.Net.SASL.Password = password[0]
	}
	if ca := opts["ca"]; len(ca) > 0 {
		caCertPool, err := makeCertPool(ca)
		if err != nil {
			return nil, fmt.Errorf("failed to load kafka root CA: %s", err)
		}
		conf.Net.TLS.Enable = true
		if conf.Net.TLS.Config == nil {
			conf.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
		}
		conf.Net.TLS.Config.RootCAs = caCertPool
	}
	if key, cert := opts["key"], opts["cert"]; len(key) > 1 && len(cert) > 1 {
		certs, err := makeKeyPair(key[0], cert[0])
		if err != nil {
			return nil, fmt.Errorf("failed to load kafka TLS keypair: %s", err)
		}
		conf.Net.TLS.Enable = true
		if conf.Net.TLS.Config == nil {
			conf.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
		}
		conf.Net.TLS.Config.Certificates = []tls.Certificate{certs}
	}
	return &kafkaSinkConfiguration{
		config:  conf,
		brokers: kafkaBrokers,
		topic:   topic,
	}, nil
}

func NewKafkaClient(uri *url.URL, topicType string) (KafkaClient, error) {

	config, err := buildConfig(uri, topicType)
	if err != nil {
		return nil, fmt.Errorf("Failed to configure Producer: - %v", err)
	}

	// TODO: adapt logger
	//sarama.Logger = &GologAdapterLogger{}.(sarama.StdLogger)

	// set up producer of kafka server.
	sinkProducer, err := setupProducer(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to setup Producer: - %v", err)
	}

	go func() {
		for sinkProducer.Errors() != nil && sinkProducer.Successes() != nil {
			select {
			case e := <-sinkProducer.Errors():
				glog.V(3).Infof("failed to produce kafka messages %s", e)
			case <-sinkProducer.Successes():
			}
		}
	}()

	return &kafkaSink{
		producer:  sinkProducer,
		dataTopic: config.topic,
	}, nil
}
