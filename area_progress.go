package chunkdb

import "math/big"

func areaCheckpoint(check func() error) error {
	if check != nil {
		return check()
	}
	return nil
}

func (r areaPageRequest) boxRank(point [3]int64) *big.Int {
	rank := new(big.Int)
	for axis := range r.dims {
		width := new(big.Int).Sub(big.NewInt(r.last[axis]), big.NewInt(r.first[axis]))
		width.Add(width, big.NewInt(1))
		offset := new(big.Int).Sub(big.NewInt(point[axis]), big.NewInt(r.first[axis]))
		rank.Mul(rank, width).Add(rank, offset)
	}
	return rank
}

func (r areaPageRequest) boxProgress(cursor *[3]int64, limit uint64) bool {
	total := big.NewInt(1)
	for axis := range r.dims {
		width := new(big.Int).Sub(big.NewInt(r.last[axis]), big.NewInt(r.first[axis]))
		total.Mul(total, width.Add(width, big.NewInt(1)))
	}
	start := new(big.Int)
	if r.after != nil {
		start.Add(r.boxRank(*r.after), big.NewInt(1))
	}
	visited := new(big.Int).Sub(total, start)
	if cursor != nil {
		end := r.boxRank(*cursor)
		if end.Cmp(start) < 0 || new(big.Int).Add(end, big.NewInt(1)).Cmp(total) >= 0 {
			return false
		}
		visited.Sub(end, start).Add(visited, big.NewInt(1))
	}
	return visited.Sign() >= 0 && visited.Cmp(new(big.Int).SetUint64(limit)) <= 0
}

func (r areaPageRequest) lastPoint() [3]int64 {
	var point [3]int64
	for axis := range r.dims {
		_, point[axis] = r.bounds(point, axis)
	}
	return point
}

// Each step admits a complete fastest-axis interval, never one cell at a time.
func (r areaPageRequest) ballProgress(cursor *[3]int64, limit uint64, check func() error) (bool, error) {
	point, exists := r.firstPoint(), true
	if r.after != nil {
		point, exists = r.next(*r.after)
	}
	if !exists {
		return cursor == nil, nil
	}
	end := r.lastPoint()
	if cursor != nil {
		end = *cursor
		if areaPointLess(end, point, r.dims) {
			return false, nil
		}
		if _, following := r.next(end); !following {
			return false, nil
		}
	}
	remaining := new(big.Int).SetUint64(limit)
	for {
		if err := areaCheckpoint(check); err != nil {
			return false, err
		}
		rowEnd := point
		_, rowEnd[r.dims-1] = r.bounds(point, r.dims-1)
		sameRow := true
		for axis := 0; axis < r.dims-1; axis++ {
			if point[axis] != end[axis] {
				sameRow = false
				break
			}
		}
		if sameRow {
			rowEnd[r.dims-1] = end[r.dims-1]
		}
		width := new(big.Int).Sub(big.NewInt(rowEnd[r.dims-1]), big.NewInt(point[r.dims-1]))
		width.Add(width, big.NewInt(1))
		if width.Sign() <= 0 || width.Cmp(remaining) > 0 {
			return false, nil
		}
		remaining.Sub(remaining, width)
		if rowEnd == end {
			return true, nil
		}
		point, exists = r.next(rowEnd)
		if !exists {
			return false, nil
		}
	}
}

func (r areaPageRequest) validProgress(cursor *[3]int64, limit uint64, check func() error) (bool, error) {
	if err := areaCheckpoint(check); err != nil {
		return false, err
	}
	if cursor != nil && (!r.contains(*cursor) || r.after != nil && !areaPointLess(*r.after, *cursor, r.dims)) {
		return false, nil
	}
	var valid bool
	var err error
	if r.radius == nil {
		valid = r.boxProgress(cursor, limit)
	} else {
		valid, err = r.ballProgress(cursor, limit, check)
	}
	if err != nil {
		return false, err
	}
	if err := areaCheckpoint(check); err != nil {
		return false, err
	}
	return valid, nil
}
