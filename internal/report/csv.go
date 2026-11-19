package report

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

func ParseCSV(ctx context.Context, data io.Reader, snapshot *Snapshot) error {
	input := csv.NewReader(data)
	input.FieldsPerRecord = 3
	input.ReuseRecord = true
	header, err := input.Read()
	if err != nil {
		return fmt.Errorf("CSV header: %w", err)
	}
	if !slices.Equal(header, []string{"publisher", "impressions", "revenue_usd"}) {
		return errors.New("unexpected CSV header")
	}
	for line := 2; ; line++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		row, err := input.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("CSV record %d: %w", line, err)
		}
		impressions, err := strconv.ParseInt(row[1], 10, 64)
		if err != nil {
			return fmt.Errorf("CSV record %d impressions: %w", line, err)
		}
		micros, err := usdMicros(row[2])
		if err != nil {
			return fmt.Errorf("CSV record %d revenue: %w", line, err)
		}
		if err := snapshot.add(row[0], impressions, micros); err != nil {
			return fmt.Errorf("CSV record %d: %w", line, err)
		}
	}
}

func usdMicros(value string) (int64, error) {
	const scale = 1_000_000
	whole, fraction, hasFraction := strings.Cut(value, ".")
	dollars, err := strconv.ParseUint(whole, 10, 64)
	if err != nil || dollars > math.MaxInt64/scale {
		return 0, errors.New("invalid USD amount")
	}
	var micros uint64
	if hasFraction {
		if len(fraction) == 0 || len(fraction) > 6 {
			return 0, errors.New("USD amount needs one to six fractional digits")
		}
		micros, err = strconv.ParseUint(fraction, 10, 64)
		if err != nil {
			return 0, errors.New("invalid USD fraction")
		}
		for i := len(fraction); i < 6; i++ {
			micros *= 10
		}
	}
	total := dollars*scale + micros
	if total > math.MaxInt64 {
		return 0, errors.New("USD amount overflow")
	}
	return int64(total), nil
}
