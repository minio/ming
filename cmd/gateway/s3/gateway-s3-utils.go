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

package s3

import (
	minio "github.com/minio/minio/cmd"
)

// List of header keys to be filtered, usually
// from all S3 API http responses.
var defaultFilterKeys = []string{
	"Connection",
	"Transfer-Encoding",
	"Accept-Ranges",
	"Date",
	"Server",
	"Vary",
	"x-amz-bucket-region",
	"x-amz-request-id",
	"x-amz-id-2",
	"Content-Security-Policy",
	"X-Xss-Protection",

	// Add new headers to be ignored.
}

// FromGatewayObjectPart converts ObjectInfo for custom part stored as object to PartInfo
func FromGatewayObjectPart(partID int, oi minio.ObjectInfo) (pi minio.PartInfo) {
	return minio.PartInfo{
		Size:         oi.Size,
		ETag:         minio.CanonicalizeETag(oi.ETag),
		LastModified: oi.ModTime,
		PartNumber:   partID,
	}
}
