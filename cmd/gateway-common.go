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
	"strings"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
)

// parse gateway sse env variable
func parseGatewaySSE(s string) (minio.GatewaySSE, error) {
	l := strings.Split(s, ";")
	var gwSlice minio.GatewaySSE
	for _, val := range l {
		v := strings.ToUpper(val)
		switch v {
		case "":
			continue
		case minio.GatewaySSES3:
			fallthrough
		case minio.GatewaySSEC:
			gwSlice = append(gwSlice, v)
			continue
		default:
			return nil, config.ErrInvalidGWSSEValue(nil).Msg("gateway SSE cannot be (%s) ", v)
		}
	}
	return gwSlice, nil
}

// handle gateway env vars
func gatewayHandleEnvVars() {
	// Handle common env vars.
	minio.HandleCommonEnvVars()

	if !minio.GlobalActiveCred.IsValid() {
		logger.Fatal(config.ErrInvalidCredentials(nil),
			"Unable to validate credentials inherited from the shell environment")
	}

	gwsseVal := env.Get("MINIO_GATEWAY_SSE", "")
	if gwsseVal != "" {
		var err error
		minio.GlobalGatewaySSE, err = parseGatewaySSE(gwsseVal)
		if err != nil {
			logger.Fatal(err, "Unable to parse MINIO_GATEWAY_SSE value (`%s`)", gwsseVal)
		}
	}
}
