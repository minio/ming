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
	"fmt"
	"strings"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/color"
)

// Prints the formatted startup message.
func printGatewayStartupMessage(apiEndPoints []string, backendType string) {
	strippedAPIEndpoints := minio.StripStandardPorts(apiEndPoints)
	// If cache layer is enabled, print cache capacity.
	cacheAPI := minio.NewCachedObjectLayerFn()
	if cacheAPI != nil {
		minio.PrintCacheStorageInfo(cacheAPI.StorageInfo(minio.GlobalContext))
	}
	// Prints credential.
	printGatewayCommonMsg(strippedAPIEndpoints)

	// Prints `mc` cli configuration message chooses
	// first endpoint as default.
	minio.PrintCLIAccessMsg(strippedAPIEndpoints[0], fmt.Sprintf("my%s", backendType))

	// Prints documentation message.
	minio.PrintObjectAPIMsg()

	// SSL is configured reads certification chain, prints
	// authority and expiry.
	if color.IsTerminal() && !minio.GlobalCLIContext.Anonymous {
		if minio.GlobalIsTLS {
			minio.PrintCertificateMsg(minio.GlobalPublicCerts)
		}
	}
}

// Prints common server startup message. Prints credential, region and browser access.
func printGatewayCommonMsg(apiEndpoints []string) {
	// Get saved credentials.
	cred := minio.GlobalActiveCred

	apiEndpointStr := strings.Join(apiEndpoints, "  ")

	// Colorize the message and print.
	minio.LogStartupMessage(color.Blue("Endpoint: ") + color.Bold(fmt.Sprintf("%s ", apiEndpointStr)))
	if color.IsTerminal() && !minio.GlobalCLIContext.Anonymous {
		minio.LogStartupMessage(color.Blue("RootUser: ") + color.Bold(fmt.Sprintf("%s ", cred.AccessKey)))
		minio.LogStartupMessage(color.Blue("RootPass: ") + color.Bold(fmt.Sprintf("%s ", cred.SecretKey)))
	}
	minio.PrintEventNotifiers()

	if minio.GlobalBrowserEnabled {
		minio.LogStartupMessage(color.Blue("\nBrowser Access:"))
		minio.LogStartupMessage(fmt.Sprintf(minio.GetFormatStr(len(apiEndpointStr), 3), apiEndpointStr))
	}
}
