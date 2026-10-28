package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
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
	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/livepeer/clearinghouse/internal/version"
)

type Common struct {
	DBPath     string `name:"db-path" default:"clearinghouse.db" descr:"Accounting SQLite file"`
	ConfigFile string `optional:"true" file:"true" descr:"Configuration file"`
}

func (p Common) configPath() string { return p.ConfigFile }

type HTTPBind struct{ netip.AddrPort }

func (b *HTTPBind) UnmarshalText(text []byte) error {
	raw := string(text)
	if raw == "" {
		b.AddrPort = netip.AddrPort{}
		return nil
	}
	if strings.HasPrefix(raw, ":") {
		raw = "127.0.0.1" + raw
	}
	addr, err := netip.ParseAddrPort(raw)
	if err != nil {
		return err
	}
	if addr.Port() == 0 {
		return errors.New("TCP port must be between 1 and 65535")
	}
	b.AddrPort = addr
	return nil
}

type ServeParams struct {
	Common
	EnableMetrics         HTTPBind      `optional:"true" descr:"Private Prometheus metrics listener; :port binds to loopback"`
	EnableAuthWebhook     HTTPBind      `optional:"true" descr:"Run signer authorization HTTP server on IP:port; :port binds to 127.0.0.1"`
	EnableManagementAPI   HTTPBind      `optional:"true" descr:"Run management HTTP server on IP:port; :port binds to 127.0.0.1"`
	UnsafeHTTPBind        bool          `name:"unsafe-http-bind" optional:"true" descr:"Allow HTTP servers to bind to a non-loopback IP"`
	EnableKafka           bool          `optional:"true" descr:"Run embedded Kafka broker"`
	EnableAccounting      bool          `optional:"true" descr:"Run accounting service"`
	KafkaBroker           string        `optional:"true" descr:"External Kafka broker address (host:port)"`
	EnableOnchainListener bool          `optional:"true" descr:"Run on-chain RPC listener"`
	CredsFile             string        `name:"creds-file" optional:"true" file:"true" descr:"JSON or TOML service credential registry"`
	KafkaBind             string        `default:"127.0.0.1:9092" descr:"Embedded broker IP:port (loopback or private)"`
	KafkaAdvertisedAddr   string        `optional:"true" descr:"Embedded broker host:port advertised to clients; defaults to KafkaBind"`
	KafkaTopic            string        `default:"livepeer-signing" descr:"Kafka topic for issued tickets"`
	MaxKafkaLag           time.Duration `default:"60s" descr:"Maximum Kafka consumer processing delay before signing pauses (minimum 1s)"`
	RPCURL                string        `name:"rpc-url" optional:"true" secret:"true" descr:"On-chain RPC URL"`
	RPCURLFile            string        `name:"rpc-url-file" secretfor:"RPCURL" descr:"File containing the on-chain RPC URL"`
	ChainID               string        `name:"chain-id" optional:"true" descr:"Optional assertion for the RPC chain ID"`
	TicketBroker          string        `name:"ticket-broker" optional:"true" descr:"TicketBroker override; resolved automatically on Arbitrum One"`
	SignerAddresses       []string      `optional:"true" descr:"Signer addresses, comma-separated (required for on-chain listener)"`
	StartBlock            *int64        `optional:"true" descr:"First block for a new chain checkpoint; defaults to the current head"`
	Confirmations         int64         `default:"64" descr:"Blocks to wait before processing on-chain events"`
	PollInterval          time.Duration `default:"5s" descr:"On-chain listener polling interval"`
	BlockBatchSize        int64         `default:"2000" descr:"Maximum blocks per on-chain scan"`
	ReorgLookback         int64         `default:"256" descr:"Maximum blocks to search for a common ancestor after a reorg"`
}

func (p ServeParams) chainConfig() chain.Config {
	return chain.Config{URL: p.RPCURL, ChainID: p.ChainID, Contract: p.TicketBroker, Senders: p.SignerAddresses, Start: p.StartBlock, Confirmations: p.Confirmations, Poll: p.PollInterval, BatchSize: p.BlockBatchSize, Lookback: p.ReorgLookback}
}

func (p ServeParams) Validate() error {
	if p.EnableMetrics.IsValid() {
		if !p.EnableMetrics.Addr().IsLoopback() && !p.UnsafeHTTPBind {
			return errors.New("metrics bind must be loopback; use --unsafe-http-bind to override")
		}
		for _, b := range []HTTPBind{p.EnableAuthWebhook, p.EnableManagementAPI} {
			if b.IsValid() && b.Port() == p.EnableMetrics.Port() {
				return errors.New("metrics requires a separate TCP port")
			}
		}
	}
	webhookEnabled := p.EnableAuthWebhook.IsValid()
	managementEnabled := p.EnableManagementAPI.IsValid()
	if !webhookEnabled && !managementEnabled && !p.EnableKafka && !p.EnableAccounting && !p.EnableOnchainListener {
		return errors.New("enable at least one of --enable-auth-webhook, --enable-management-api, --enable-kafka, --enable-accounting, --enable-onchain-listener")
	}
	if managementEnabled {
		if !p.EnableManagementAPI.Addr().IsLoopback() && !p.UnsafeHTTPBind {
			return errors.New("management bind must be a loopback IP; use --unsafe-http-bind to allow a non-loopback address")
		}
		if webhookEnabled && p.EnableManagementAPI.Port() == p.EnableAuthWebhook.Port() {
			return errors.New("management and auth webhook must use separate TCP ports")
		}
	}
	if p.EnableKafka && p.KafkaBroker != "" {
		return errors.New("--kafka-broker cannot be used with --enable-kafka")
	}
	if p.KafkaAdvertisedAddr != "" {
		if !p.EnableKafka {
			return errors.New("--kafka-advertised-addr requires --enable-kafka")
		}
		host, port, err := net.SplitHostPort(p.KafkaAdvertisedAddr)
		n, portErr := strconv.ParseUint(port, 10, 16)
		if err != nil || host == "" || portErr != nil || n == 0 {
			return fmt.Errorf("invalid Kafka advertised address %q: expected host:port with a port between 1 and 65535", p.KafkaAdvertisedAddr)
		}
	}
	if p.EnableAccounting && !p.EnableKafka && p.KafkaBroker == "" {
		return errors.New("--kafka-broker required for accounting without embedded Kafka")
	}
	if p.KafkaBroker != "" {
		host, port, err := net.SplitHostPort(p.KafkaBroker)
		n, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || n < 1 || n > 65535 {
			return fmt.Errorf("invalid Kafka broker address %q: expected host:port", p.KafkaBroker)
		}
	}
	if (webhookEnabled || managementEnabled || p.EnableKafka) && p.CredsFile == "" {
		return errors.New("--creds-file is required for management, webhook, or embedded Kafka")
	}
	if p.EnableKafka || p.EnableAccounting || webhookEnabled {
		if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$`).MatchString(p.KafkaTopic) {
			return errors.New("invalid Kafka topic")
		}
	}
	if webhookEnabled {
		if !p.EnableAuthWebhook.Addr().IsLoopback() && !p.UnsafeHTTPBind {
			return errors.New("auth webhook bind must be a loopback IP; use --unsafe-http-bind to allow a non-loopback address")
		}
	}
	if p.EnableKafka {
		host, port, err := net.SplitHostPort(p.KafkaBind)
		if err != nil {
			return err
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("kafka requires a fixed TCP port between 1 and 65535")
		}
		ip := net.ParseIP(host)
		if ip == nil || (!ip.IsLoopback() && !ip.IsPrivate()) {
			return errors.New("kafka bind must be a concrete loopback or private IP (no public or wildcard bind)")
		}
	}
	if p.MaxKafkaLag < store.MinKafkaLag {
		return errors.New("--max-kafka-lag must be at least 1s")
	}
	if p.EnableOnchainListener {
		if p.RPCURL == "" {
			return errors.New("RPC URL required: set CLEARINGHOUSE_RPC_URL or --rpc-url-file")
		}
		return p.chainConfig().Validate()
	}
	return nil
}

func Serve(ctx context.Context, p ServeParams) error {
	if err := p.Validate(); err != nil {
		return err
	}
	var registry *serviceauth.Registry
	if p.CredsFile != "" {
		var err error
		registry, err = serviceauth.Load(p.CredsFile)
		if err != nil {
			return err
		}
	}
	if p.EnableManagementAPI.IsValid() && registry.Count("management") == 0 {
		return errors.New("management API requires a management credential")
	}
	if p.EnableAuthWebhook.IsValid() && registry.Count("webhook") == 0 {
		return errors.New("auth webhook requires a webhook credential")
	}
	access, kafkaDialer, err := p.kafkaSecurity(registry)
	if err != nil {
		return err
	}
	webhookBind := ""
	if p.EnableAuthWebhook.IsValid() {
		webhookBind = p.EnableAuthWebhook.String()
	}
	managementBind := ""
	if p.EnableManagementAPI.IsValid() {
		managementBind = p.EnableManagementAPI.String()
	}
	var db *store.Store
	if webhookBind != "" || managementBind != "" || p.EnableAccounting || p.EnableOnchainListener {
		var err error
		db, err = store.Open(ctx, p.DBPath, true)
		if err != nil {
			return err
		}
		defer db.Close()
		db.KafkaTopic = p.KafkaTopic
		db.MaxKafkaLag = p.MaxKafkaLag
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
	if p.EnableAccounting {
		// Invalidate previous-run evidence before any webhook can start.
		if err := db.SetKafkaLag(ctx, p.KafkaTopic, store.KafkaLag{}); err != nil {
			return err
		}
	}
	var broker *minikafka.Broker
	brokerAddr := p.KafkaBroker
	if p.EnableKafka {
		var err error
		broker, err = kafka.OpenBroker(ctx, p.KafkaBind, p.KafkaAdvertisedAddr, p.KafkaTopic, filepath.Dir(p.DBPath), access)
		if err != nil {
			return err
		}
		defer broker.Close()
		brokerAddr = broker.Addr()
		if p.EnableAccounting && p.KafkaAdvertisedAddr != "" {
			// Metadata advertises the client endpoint, but accounting uses the
			// embedded broker's local plaintext listener for every connection.
			dial := kafkaDialer.DialFunc
			kafkaDialer.DialFunc = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dial(ctx, network, brokerAddr)
			}
		}
	}
	var telemetry *metrics.Metrics
	if p.EnableMetrics.IsValid() {
		telemetry = metrics.New(version.Version, version.Commit)
		if db != nil {
			db.Metrics = telemetry
		}
	}
	k := &kafka.Listener{DB: db, Broker: brokerAddr, Topic: p.KafkaTopic, Dialer: kafkaDialer, Metrics: telemetry}
	c := &chain.Listener{DB: db, Config: p.chainConfig(), Metrics: telemetry}
	done := make(chan error, 7)
	count := 0
	start := func(fn func(context.Context) error) { count++; go func() { done <- fn(ctx) }() }
	if telemetry != nil {
		ln, err := net.Listen("tcp", p.EnableMetrics.String())
		if err != nil {
			return err
		}
		defer ln.Close()
		monitorCtx, stopMonitor := context.WithCancel(ctx)
		defer stopMonitor()
		monitorDone := make(chan struct{})
		go func() { defer close(monitorDone); monitorMetrics(monitorCtx, p, db, k, c, telemetry) }()
		defer func() { stopMonitor(); <-monitorDone }()
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", telemetry.Handler())
		start(func(ctx context.Context) error { return serveHTTP(ctx, ln, mux) })
	}
	var webhookListener, managementListener net.Listener
	if webhookBind != "" {
		ln, err := net.Listen("tcp", webhookBind)
		if err != nil {
			return err
		}
		webhookListener = ln
		defer webhookListener.Close()
	}
	if managementBind != "" {
		ln, err := net.Listen("tcp", managementBind)
		if err != nil {
			return err
		}
		managementListener = ln
		defer managementListener.Close()
	}
	if webhookListener != nil {
		start(func(ctx context.Context) error {
			return serveHTTP(ctx, webhookListener, telemetry.HTTP("webhook", authWebhookHandler(ctx, db, registry)))
		})
		slog.Info("auth webhook listening", "bind", webhookListener.Addr().String())
	}
	if managementListener != nil {
		start(func(ctx context.Context) error {
			return serveHTTP(ctx, managementListener, telemetry.HTTP("management", managementHandler(ctx, db, registry)))
		})
		slog.Info("management API listening", "bind", managementListener.Addr().String())
	}
	if p.EnableKafka {
		start(broker.Serve)
		slog.Info("Kafka broker listening", "bind", broker.Addr())
	}
	if p.EnableAccounting {
		start(k.Run)
		start(func(ctx context.Context) error { return k.MonitorLag(ctx, p.MaxKafkaLag) })
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

func serveHTTP(ctx context.Context, ln net.Listener, handler http.Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
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
}

func authWebhookHandler(ctx context.Context, db *store.Store, registry *serviceauth.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/signer/authorize", auth.Handler(db, registry))
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ctx.Err() != nil || db.DB.PingContext(r.Context()) != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
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
