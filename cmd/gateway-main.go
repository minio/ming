// This file is part of MinIO Gateway
// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/certs"
	"github.com/minio/minio/pkg/color"
	"github.com/minio/minio/pkg/env"
)

// GatewayLocker implements custom NewNSLock implementation
type GatewayLocker struct {
	minio.ObjectLayer
	nsMutex *minio.NSLockMap
}

// NewNSLock - implements gateway level locker
func (l *GatewayLocker) NewNSLock(bucket string, objects ...string) minio.RWLocker {
	return l.nsMutex.NewNSLock(nil, bucket, objects...)
}

// Walk - implements common gateway level Walker, to walk on all objects recursively at a prefix
func (l *GatewayLocker) Walk(ctx context.Context, bucket, prefix string, results chan<- minio.ObjectInfo, opts minio.ObjectOptions) error {
	walk := func(ctx context.Context, bucket, prefix string, results chan<- minio.ObjectInfo) error {
		go func() {
			// Make sure the results channel is ready to be read when we're done.
			defer close(results)

			var marker string

			for {
				// set maxKeys to '0' to list maximum possible objects in single call.
				loi, err := l.ObjectLayer.ListObjects(ctx, bucket, prefix, marker, "", 0)
				if err != nil {
					logger.LogIf(ctx, err)
					return
				}
				marker = loi.NextMarker
				for _, obj := range loi.Objects {
					select {
					case results <- obj:
					case <-ctx.Done():
						return
					}
				}
				if !loi.IsTruncated {
					break
				}
			}
		}()
		return nil
	}

	if err := l.ObjectLayer.Walk(ctx, bucket, prefix, results, opts); err != nil {
		if _, ok := err.(minio.NotImplemented); ok {
			return walk(ctx, bucket, prefix, results)
		}
		return err
	}

	return nil
}

// NewGatewayLayerWithLocker - initialize gateway with locker.
func NewGatewayLayerWithLocker(gwLayer minio.ObjectLayer) minio.ObjectLayer {
	return &GatewayLocker{ObjectLayer: gwLayer, nsMutex: minio.NewNSLock(false)}
}

// RegisterGatewayCommand registers a new command for gateway.
func RegisterGatewayCommand(cmd cli.Command) error {
	cmd.Flags = append(cmd.Flags, GlobalFlags...)
	Commands = append(Commands, cmd)
	return nil
}

// ParseGatewayEndpoint - Return endpoint.
func ParseGatewayEndpoint(arg string) (endPoint string, secure bool, err error) {
	schemeSpecified := len(strings.Split(arg, "://")) > 1
	if !schemeSpecified {
		// Default connection will be "secure".
		arg = "https://" + arg
	}

	u, err := url.Parse(arg)
	if err != nil {
		return "", false, err
	}

	switch u.Scheme {
	case "http":
		return u.Host, false, nil
	case "https":
		return u.Host, true, nil
	default:
		return "", false, fmt.Errorf("Unrecognized scheme %s", u.Scheme)
	}
}

// ValidateGatewayArguments - Validate gateway arguments.
func ValidateGatewayArguments(serverAddr, endpointAddr string) error {
	if err := minio.CheckLocalServerAddr(serverAddr); err != nil {
		return err
	}

	if endpointAddr != "" {
		// Reject the endpoint if it points to the gateway handler itself.
		sameTarget, err := minio.SameLocalAddrs(endpointAddr, serverAddr)
		if err != nil {
			return err
		}
		if sameTarget {
			return fmt.Errorf("endpoint points to the local gateway")
		}
	}
	return nil
}

var (
	globalDeploymentID string
	globalConsoleSys   *minio.HTTPConsoleLoggerSys
)

// StartGateway - handler for 'ming <name>'.
func StartGateway(ctx *cli.Context, gw Gateway) {
	defer minio.GlobalDNSCache.Stop()

	// This is only to uniquely identify each gateway deployments.
	globalDeploymentID = env.Get("MINIO_GATEWAY_DEPLOYMENT_ID", minio.MustGetUUID())
	logger.SetDeploymentID(globalDeploymentID)

	if gw == nil {
		logger.FatalIf(minio.ErrUnexpected, "Gateway implementation not initialized")
	}

	// Validate if we have access, secret set through environment.
	gatewayName := gw.Name()
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, gatewayName, 1)
	}

	minio.GlobalGatewayName = gatewayName

	// Initialize globalConsoleSys system
	globalConsoleSys = minio.NewConsoleLogger(minio.GlobalContext)
	logger.AddTarget(globalConsoleSys)

	// Handle common command args.
	minio.HandleCommonCmdArgs(ctx)

	// Check and load TLS certificates.
	var err error
	minio.GlobalPublicCerts, minio.GlobalTLSCerts, minio.GlobalIsTLS, err = minio.GetTLSConfig()
	logger.FatalIf(err, "Invalid TLS certificate file")

	// Check and load Root CAs.
	minio.GlobalRootCAs, err = certs.GetRootCAs(minio.GlobalCertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Add the minio.Global public crts as part of minio.Global root CAs
	for _, publicCrt := range minio.GlobalPublicCerts {
		minio.GlobalRootCAs.AddCert(publicCrt)
	}

	// Register root CAs for remote ENVs
	env.RegisterGlobalCAs(minio.GlobalRootCAs)

	// Initialize all help
	minio.InitHelp()

	// Get port to listen on from gateway address
	minio.GlobalMinioHost, minio.GlobalMinioPort = minio.MustSplitHostPort(minio.GlobalCLIContext.Addr)

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(minio.CheckPortAvailability(minio.GlobalMinioHost, minio.GlobalMinioPort), "Unable to start the gateway")

	minio.GlobalMinioEndpoint = func() string {
		host := minio.GlobalMinioHost
		if host == "" {
			host = minio.SortIPs(minio.LocalIP4.ToSlice())[0]
		}
		return fmt.Sprintf("%s://%s", minio.GetURLScheme(minio.GlobalIsTLS), net.JoinHostPort(host, minio.GlobalMinioPort))
	}()

	// Handle gateway specific env
	gatewayHandleEnvVars()

	// Set system resources to maximum.
	minio.SetMaxResources()

	// Set when gateway is enabled
	minio.GlobalIsGateway = true

	enableConfigOps := false

	// TODO: We need to move this code with globalConfigSys.Init()
	// for now keep it here such that "s3" gateway layer initializes
	// itself properly when KMS is set.

	// Initialize server config.
	srvCfg := minio.NewServerConfig()

	// Override any values from ENVs.
	minio.LookupConfigs(srvCfg, nil)

	// hold the mutex lock before a new config is assigned.
	minio.GlobalServerConfigMu.Lock()
	minio.GlobalServerConfig = srvCfg
	minio.GlobalServerConfigMu.Unlock()

	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	// avoid URL path encoding minio/minio#8950
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	if minio.GlobalEtcdClient != nil {
		// Enable STS router if etcd is enabled.
		minio.RegisterSTSRouter(router)
	}

	enableIAMOps := minio.GlobalEtcdClient != nil

	// Enable IAM admin APIs if etcd is enabled, if not just enable basic
	// operations such as profiling, server info etc.
	minio.RegisterAdminRouter(router, enableConfigOps, enableIAMOps)

	// Add healthcheck router
	minio.RegisterHealthCheckRouter(router)

	// Add server metrics router
	minio.RegisterMetricsRouter(router)

	// Register web router when its enabled.
	if minio.GlobalBrowserEnabled {
		logger.FatalIf(minio.RegisterWebRouter(router), "Unable to configure web browser")
	}

	// Add API router.
	minio.RegisterAPIRouter(router)

	// Use all the middlewares
	router.Use(minio.GlobalHandlers...)

	var getCert certs.GetCertificateFunc
	if minio.GlobalTLSCerts != nil {
		getCert = minio.GlobalTLSCerts.GetCertificate
	}

	httpServer := xhttp.NewServer([]string{minio.GlobalCLIContext.Addr},
		minio.SetCriticalErrorHandler(minio.CorsHandler(router)), getCert)
	httpServer.BaseContext = func(listener net.Listener) context.Context {
		return minio.GlobalContext
	}
	go func() {
		minio.GlobalHTTPServerErrorCh <- httpServer.Start()
	}()

	minio.GlobalObjLayerMutex.Lock()
	minio.GlobalHTTPServer = httpServer
	minio.GlobalObjLayerMutex.Unlock()

	signal.Notify(minio.GlobalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	newObject, err := gw.NewGatewayLayer(*minio.GlobalActiveCred)
	if err != nil {
		minio.GlobalHTTPServer.Shutdown()
		logger.FatalIf(err, "Unable to initialize gateway backend")
	}
	newObject = NewGatewayLayerWithLocker(newObject)

	// Calls all New() for all sub-systems.
	minio.NewAllSubsystems()

	// Once endpoints are finalized, initialize the new object api in safe mode.
	minio.GlobalObjLayerMutex.Lock()
	minio.GlobalObjectAPI = newObject
	minio.GlobalObjLayerMutex.Unlock()

	if gatewayName == NASBackendGateway {
		buckets, err := newObject.ListBuckets(minio.GlobalContext)
		if err != nil {
			logger.Fatal(err, "Unable to list buckets")
		}
		logger.FatalIf(minio.GlobalNotificationSys.Init(minio.GlobalContext, buckets, newObject), "Unable to initialize notification system")
	}

	if minio.GlobalEtcdClient != nil {
		// ****  WARNING ****
		// Migrating to encrypted backend on etcd should happen before initialization of
		// IAM sub-systems, make sure that we do not move the above codeblock elsewhere.
		logger.FatalIf(minio.MigrateIAMConfigsEtcdToEncrypted(minio.GlobalContext, minio.GlobalEtcdClient),
			"Unable to handle encrypted backend for iam and policies")
	}

	if enableIAMOps {
		// Initialize users credentials and policies in background.
		minio.GlobalIAMSys.InitStore(newObject)

		go minio.GlobalIAMSys.Init(minio.GlobalContext, newObject)
	}

	if minio.GlobalCacheConfig.Enabled {
		// initialize the new disk cache objects.
		var cacheAPI minio.CacheObjectLayer
		cacheAPI, err = minio.NewServerCacheObjects(minio.GlobalContext, *minio.GlobalCacheConfig)
		logger.FatalIf(err, "Unable to initialize disk caching")

		minio.GlobalObjLayerMutex.Lock()
		minio.GlobalCacheObjectAPI = cacheAPI
		minio.GlobalObjLayerMutex.Unlock()
	}

	// Populate existing buckets to the etcd backend
	if minio.GlobalDNSConfig != nil {
		buckets, err := newObject.ListBuckets(minio.GlobalContext)
		if err != nil {
			logger.Fatal(err, "Unable to list buckets")
		}
		minio.InitFederatorBackend(buckets, newObject)
	}

	// Verify if object layer supports
	// - encryption
	// - compression
	minio.VerifyObjectLayerFeatures("gateway "+gatewayName, newObject)

	// Prints the formatted startup message once object layer is initialized.
	if !minio.GlobalCLIContext.Quiet {
		mode := minio.GlobalMinioModeGatewayPrefix + gatewayName
		// Check update mode.
		minio.CheckUpdate(mode)

		// Print a warning message if gateway is not ready for production before the startup banner.
		if !gw.Production() {
			minio.LogStartupMessage(color.Yellow("               *** Warning: Not Ready for Production ***"))
		}

		// Print gateway startup message.
		printGatewayStartupMessage(minio.GetAPIEndpoints(), gatewayName)
	}

	minio.HandleSignals()
}
