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
	"github.com/livepeer/clearinghouse/internal/store"
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
	EnableAuthWebhook     HTTPBind      `optional:"true" descr:"Run signer authorization HTTP server on IP:port; :port binds to 127.0.0.1"`
	UnsafeHTTPBind        bool          `name:"unsafe-http-bind" optional:"true" descr:"Allow the auth webhook to bind to a non-loopback IP"`
	EnableKafka           bool          `optional:"true" descr:"Run embedded Kafka broker"`
	EnableAccounting      bool          `optional:"true" descr:"Run accounting service"`
	KafkaBrokers          []string      `optional:"true" descr:"External Kafka bootstrap addresses (host:port)"`
	EnableOnchainListener bool          `optional:"true" descr:"Run on-chain RPC listener"`
	WebhookToken          string        `optional:"true" secret:"true" descr:"Signer-to-clearinghouse shared token"`
	WebhookTokenFile      string        `secretfor:"WebhookToken" descr:"File containing the signer-to-clearinghouse shared token"`
	KafkaBind             string        `default:"127.0.0.1:9092" descr:"Embedded broker IP:port (loopback or private)"`
	KafkaTopic            string        `default:"livepeer-signing" descr:"Kafka topic for issued tickets"`
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
	webhookEnabled := p.EnableAuthWebhook.IsValid()
	if !webhookEnabled && !p.EnableKafka && !p.EnableAccounting && !p.EnableOnchainListener {
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
	if webhookEnabled {
		if !p.EnableAuthWebhook.Addr().IsLoopback() && !p.UnsafeHTTPBind {
			return errors.New("auth webhook bind must be a loopback IP; use --unsafe-http-bind to allow a non-loopback address")
		}
		if p.WebhookToken == "" {
			return errors.New("webhook token required: set CLEARINGHOUSE_WEBHOOK_TOKEN or --webhook-token-file")
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
	webhookBind := ""
	if p.EnableAuthWebhook.IsValid() {
		webhookBind = p.EnableAuthWebhook.String()
	}
	var db *store.Store
	if webhookBind != "" || p.EnableAccounting || p.EnableOnchainListener {
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
	if webhookBind != "" {
		ln, err := net.Listen("tcp", webhookBind)
		if err != nil {
			return err
		}
		srv := &http.Server{Handler: authWebhookHandler(ctx, db, p.WebhookToken), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
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

func authWebhookHandler(ctx context.Context, db *store.Store, token string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/signer/authorize", auth.Handler(db, token))
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
