package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/livepeer/clearinghouse/internal/auth"
	"github.com/livepeer/clearinghouse/internal/chain"
	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/store"
)

type Common struct {
	DBPath     string `name:"db-path" env:"CLEARINGHOUSE_DB_PATH" default:"clearinghouse.db" descr:"Accounting SQLite file"`
	ConfigFile string `optional:"true" env:"CLEARINGHOUSE_CONFIG_FILE" descr:"Configuration file"`
}

func (p Common) configPath() string { return p.ConfigFile }

type ServeParams struct {
	Common
	EnableAuthWebhook     bool          `optional:"true" env:"CLEARINGHOUSE_ENABLE_AUTH_WEBHOOK" descr:"Run signer authorization HTTP server"`
	EnableKafka           bool          `optional:"true" env:"CLEARINGHOUSE_ENABLE_KAFKA" descr:"Run embedded Kafka broker"`
	EnableAccounting      bool          `optional:"true" env:"CLEARINGHOUSE_ENABLE_ACCOUNTING" descr:"Run accounting service"`
	KafkaBrokers          []string      `optional:"true" env:"CLEARINGHOUSE_KAFKA_BROKERS" descr:"External Kafka bootstrap addresses (host:port)"`
	EnableOnchainListener bool          `optional:"true" env:"CLEARINGHOUSE_ENABLE_ONCHAIN_LISTENER" descr:"Run on-chain RPC listener"`
	HTTPBind              string        `name:"http-bind" default:"127.0.0.1:8080" env:"CLEARINGHOUSE_HTTP_BIND"`
	WebhookToken          string        `optional:"true" env:"CLEARINGHOUSE_WEBHOOK_TOKEN" descr:"Signer-to-clearinghouse shared token"`
	KafkaBind             string        `default:"127.0.0.1:9092" env:"CLEARINGHOUSE_KAFKA_BIND"`
	KafkaTopic            string        `default:"livepeer-signing" env:"CLEARINGHOUSE_KAFKA_TOPIC"`
	RPCURL                string        `name:"rpc-url" optional:"true" env:"CLEARINGHOUSE_RPC_URL"`
	ChainID               string        `name:"chain-id" optional:"true" env:"CLEARINGHOUSE_CHAIN_ID" descr:"Optional assertion for the RPC chain ID"`
	TicketBroker          string        `name:"ticket-broker" optional:"true" env:"CLEARINGHOUSE_TICKET_BROKER" descr:"TicketBroker override; resolved automatically on Arbitrum One"`
	SignerAddresses       []string      `optional:"true" env:"CLEARINGHOUSE_SIGNER_ADDRESSES"`
	StartBlock            *int64        `optional:"true" env:"CLEARINGHOUSE_START_BLOCK" descr:"First block for a new chain checkpoint; defaults to the current head"`
	Confirmations         int64         `default:"64" env:"CLEARINGHOUSE_CONFIRMATIONS"`
	PollInterval          time.Duration `default:"5s" env:"CLEARINGHOUSE_POLL_INTERVAL"`
	BlockBatchSize        int64         `default:"2000" env:"CLEARINGHOUSE_BLOCK_BATCH_SIZE"`
	ReorgLookback         int64         `default:"256" env:"CLEARINGHOUSE_REORG_LOOKBACK"`
}

func (p ServeParams) chainConfig() chain.Config {
	return chain.Config{URL: p.RPCURL, ChainID: p.ChainID, Contract: p.TicketBroker, Senders: p.SignerAddresses, Start: p.StartBlock, Confirmations: p.Confirmations, Poll: p.PollInterval, BatchSize: p.BlockBatchSize, Lookback: p.ReorgLookback}
}
func (p ServeParams) Validate() error {
	if !p.EnableAuthWebhook && !p.EnableKafka && !p.EnableAccounting && !p.EnableOnchainListener {
		return errors.New("enable at least one of --enable-auth-webhook, --enable-kafka, --enable-accounting, --enable-onchain-listener")
	}
	if p.EnableKafka && len(p.KafkaBrokers) > 0 {
		return errors.New("--kafka-brokers cannot be used with --enable-kafka")
	}
	if p.EnableAccounting && !p.EnableKafka && len(p.KafkaBrokers) == 0 {
		return errors.New("--kafka-brokers required for accounting without embedded Kafka")
	}
	for _, addr := range p.KafkaBrokers {
		host, port, err := net.SplitHostPort(addr)
		n, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || n < 1 || n > 65535 {
			return fmt.Errorf("invalid Kafka broker address %q: expected host:port", addr)
		}
	}
	if p.EnableKafka || p.EnableAccounting {
		if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$`).MatchString(p.KafkaTopic) {
			return errors.New("invalid Kafka topic")
		}
	}
	if p.EnableAuthWebhook {
		if _, _, err := net.SplitHostPort(p.HTTPBind); err != nil {
			return fmt.Errorf("http bind: %w", err)
		}
		if p.WebhookToken == "" {
			return errors.New("--webhook-token is required for the auth webhook")
		}
	}
	if p.EnableKafka {
		host, port, err := net.SplitHostPort(p.KafkaBind)
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("Kafka requires a fixed TCP port between 1 and 65535")
		}
		ip := net.ParseIP(host)
		if ip == nil || (!ip.IsLoopback() && !ip.IsPrivate()) {
			return errors.New("Kafka bind must be a concrete loopback or private IP (no public or wildcard bind)")
		}
	}
	if p.EnableOnchainListener {
		if p.RPCURL == "" {
			return errors.New("--rpc-url required")
		}
		return p.chainConfig().Validate()
	}
	return nil
}

func Serve(ctx context.Context, p ServeParams) error {
	if err := p.Validate(); err != nil {
		return err
	}
	var db *store.Store
	if p.EnableAuthWebhook || p.EnableAccounting || p.EnableOnchainListener {
		var err error
		db, err = store.Open(ctx, p.DBPath, true)
		if err != nil {
			return err
		}
		defer db.Close()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var locks []*os.File
	defer func() {
		for _, f := range locks {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
		}
	}()
	for _, enabled := range []struct {
		on   bool
		path string
	}{{p.EnableAccounting, p.DBPath + ".accounting.lock"}, {p.EnableOnchainListener, p.DBPath + ".chain.lock"}, {p.EnableKafka, filepath.Join(filepath.Dir(p.DBPath), "minikafka_broker.lock")}} {
		if !enabled.on {
			continue
		}
		f, err := lock(enabled.path)
		if err != nil {
			return err
		}
		locks = append(locks, f)
	}
	var broker *minikafka.Broker
	brokers := p.KafkaBrokers
	if p.EnableKafka {
		var err error
		broker, err = kafka.OpenBroker(ctx, p.KafkaBind, p.KafkaTopic, filepath.Dir(p.DBPath))
		if err != nil {
			return err
		}
		defer broker.Close()
		brokers = []string{broker.Addr()}
	}
	k := &kafka.Listener{DB: db, Brokers: brokers, Topic: p.KafkaTopic}
	c := &chain.Listener{DB: db, Config: p.chainConfig()}
	done := make(chan error, 4)
	count := 0
	start := func(fn func(context.Context) error) { count++; go func() { done <- fn(ctx) }() }
	if p.EnableAuthWebhook {
		mux := http.NewServeMux()
		mux.Handle("POST /v1/signer/authorize", auth.Handler(db, p.WebhookToken))
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
		mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
			if ctx.Err() != nil || (p.EnableAccounting && !k.Ready.Load()) || (p.EnableOnchainListener && !c.Ready.Load()) || db.DB.PingContext(r.Context()) != nil {
				http.Error(w, "not ready", 503)
				return
			}
			w.WriteHeader(200)
		})
		ln, err := net.Listen("tcp", p.HTTPBind)
		if err != nil {
			return err
		}
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
		start(func(ctx context.Context) error {
			shutdownDone := make(chan struct{})
			go func() {
				defer close(shutdownDone)
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := srv.Shutdown(shutdownCtx); err != nil {
					_ = srv.Close()
				}
			}()
			err := srv.Serve(ln)
			cancel()
			<-shutdownDone
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
		slog.Info("auth webhook listening", "bind", ln.Addr().String())
	}
	if p.EnableKafka {
		start(broker.Serve)
		slog.Info("Kafka broker listening", "bind", broker.Addr())
	}
	if p.EnableAccounting {
		start(k.Run)
	}
	if p.EnableOnchainListener {
		start(c.Run)
	}
	var result error
	select {
	case <-ctx.Done():
	case result = <-done:
		count--
	}
	cancel()
	for range count {
		if err := <-done; result == nil && err != nil && !errors.Is(err, context.Canceled) {
			result = err
		}
	}
	if errors.Is(result, context.Canceled) {
		return nil
	}
	return result
}

func lock(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("component already running (%s): %w", strings.TrimSpace(path), err)
	}
	return f, nil
}
