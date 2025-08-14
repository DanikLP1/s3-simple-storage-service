package server

import (
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/DanikLP1/s3-storage-service/internal/db"
)

type s3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

func writeS3Error(w http.ResponseWriter, status int, code, msg, resource, reqID string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3Error{
		Code: code, Message: msg, Resource: resource, RequestID: reqID,
	})
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   S3Owner  `xml:"Owner"`
	Buckets struct {
		Bucket []S3Bucket `xml:"Bucket"`
	} `xml:"Buckets"`
}
type S3Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}
type S3Bucket struct {
	Name         string    `xml:"Name"`
	CreationDate time.Time `xml:"CreationDate"`
}

func writeListBuckets(w http.ResponseWriter, ownerID, ownerName string, buckets []S3Bucket) {
	res := ListAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: S3Owner{ID: ownerID, DisplayName: ownerName},
	}
	res.Buckets.Bucket = buckets

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(res)
}

type ListBucketResultV2 struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Xmlns                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	MaxKeys               int               `xml:"MaxKeys"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
	KeyCount              int               `xml:"KeyCount"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	CommonPrefixes        []CommonPrefix    `xml:"CommonPrefixes,omitempty"`
	Contents              []ListV2ObjectXML `xml:"Contents,omitempty"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type ListV2OwnerXML struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type ListV2ObjectXML struct {
	Key          string          `xml:"Key"`
	LastModified string          `xml:"LastModified,omitempty"` // RFC3339
	ETag         string          `xml:"ETag,omitempty"`
	Size         int64           `xml:"Size"`
	StorageClass string          `xml:"StorageClass,omitempty"`
	Owner        *ListV2OwnerXML `xml:"Owner,omitempty"`
}

func writeListObjectsV2(
	w http.ResponseWriter,
	payload ListBucketResultV2,
) {
	payload.Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(payload)
}

type LifecycleConfiguration struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}
type Rule struct {
	ID     string  `xml:"ID,omitempty"`
	Status string  `xml:"Status"` // Enabled/Disabled
	Filter *Filter `xml:"Filter,omitempty"`
	// действия
	Expiration                     *Expiration                     `xml:"Expiration,omitempty"`
	NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
}
type Filter struct {
	Prefix string `xml:"Prefix,omitempty"`
}
type Expiration struct {
	Days *int `xml:"Days,omitempty"`
}
type NoncurrentVersionExpiration struct {
	NoncurrentDays          *int `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions *int `xml:"NewerNoncurrentVersions,omitempty"`
}
type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation *int `xml:"DaysAfterInitiation,omitempty"`
}

func ruleFromXML(bucketID uint, x Rule) db.LifecycleRule {
	prefix := ""
	if x.Filter != nil {
		prefix = x.Filter.Prefix
	}
	enabled := strings.EqualFold(x.Status, "Enabled")
	r := db.LifecycleRule{BucketID: bucketID, Prefix: prefix, Enabled: enabled}
	if x.Expiration != nil {
		r.ExpireCurrentAfterDays = x.Expiration.Days
	}
	if x.NoncurrentVersionExpiration != nil {
		r.ExpireNoncurrentAfterDays = x.NoncurrentVersionExpiration.NoncurrentDays
		r.NoncurrentNewerVersionsToKeep = x.NoncurrentVersionExpiration.NewerNoncurrentVersions
	}
	// Purge delete-markers можно повесить на отдельный Rule.ID или оформить отдельным полем/конвенцией
	return r
}

func ruleToXML(r db.LifecycleRule) Rule {
	status := "Disabled"
	if r.Enabled {
		status = "Enabled"
	}
	var exp *Expiration
	if r.ExpireCurrentAfterDays != nil {
		exp = &Expiration{Days: r.ExpireCurrentAfterDays}
	}
	var nce *NoncurrentVersionExpiration
	if r.ExpireNoncurrentAfterDays != nil || r.NoncurrentNewerVersionsToKeep != nil {
		nce = &NoncurrentVersionExpiration{
			NoncurrentDays:          r.ExpireNoncurrentAfterDays,
			NewerNoncurrentVersions: r.NoncurrentNewerVersionsToKeep,
		}
	}
	return Rule{
		Status:                      status,
		Filter:                      &Filter{Prefix: r.Prefix},
		Expiration:                  exp,
		NoncurrentVersionExpiration: nce,
		// AbortIncompleteMultipartUpload можно добавить позже
	}
}
