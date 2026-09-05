package main

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ---- options -------------------------------------------------------------------------------------------------------------

type opts struct {
	to, from   string
	workers    int
	limit      int64
	excludes   []string
	checksum   bool
	dryRun     bool
	jsonOut    bool
	positional []string
}

func parseOpts(args []string) (*opts, error) {
	o := &opts{workers: 6}
	for i := 0; i < len(args); i++ {
		a := args[i]
		next := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("%s needs a value", a)
			}
			i++
			return args[i], nil
		}
		var err error
		switch {
		case a == "--to":
			o.to, err = next()
		case a == "--from":
			o.from, err = next()
		case a == "--workers":
			var v string
			v, err = next()
			if err == nil {
				fmt.Sscanf(v, "%d", &o.workers)
			}
		case a == "--limit":
			var v string
			v, err = next()
			if err == nil {
				o.limit, err = parseRate(v)
			}
		case a == "--exclude":
			var v string
			v, err = next()
			o.excludes = append(o.excludes, v)
		case a == "--checksum":
			o.checksum = true
		case a == "--dry-run":
			o.dryRun = true
		case a == "--json":
			o.jsonOut = true
		case strings.HasPrefix(a, "-"):
			return nil, fmt.Errorf("unknown option %s", a)
		default:
			o.positional = append(o.positional, a)
		}
		if err != nil {
			return nil, err
		}
	}
	if o.workers < 1 {
		o.workers = 1
	}
	return o, nil
}

func excluded(rel string, globs []string) bool {
	for _, g := range globs {
		if ok, _ := path.Match(g, rel); ok {
			return true
		}
		if ok, _ := path.Match(g, path.Base(rel)); ok {
			return true
		}
		if strings.HasSuffix(g, "/*") && strings.HasPrefix(rel, strings.TrimSuffix(g, "*")) {
			return true
		}
	}
	return false
}

// ---- local index -----------------------------------------------------------------------------------------------------------

type localFile struct {
	rel  string // forward slashes, relative to the root
	size int64
	sha  string
}

// abs rebuilds the path on disk. Only the relative path is kept: the absolute one is the same root repeated
// once per file, and over a few million of them that duplication was gigabytes of a push. Rebuilding it costs
// one join, and only at the moment a file is opened.
func (f *localFile) abs(root string) string { return filepath.Join(root, filepath.FromSlash(f.rel)) }

// walkFiles reads the tree under root with a fixed pool of workers (a directory is the unit of work) and sends
// every regular file to out, closing it when the tree is exhausted. It keeps only the directories it has still
// to visit, so the caller alone decides how much of the tree is in memory at once. The pool is fixed rather
// than a goroutine per directory: a wide tree used to spawn one for every directory it discovered, and each sat
// on its stack waiting for a turn.
func walkFiles(ctx context.Context, root string, globs []string, prog *progress, out chan<- localFile) error {
	defer close(out)
	st, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		select {
		case out <- localFile{rel: filepath.Base(root), size: st.Size()}:
		case <-ctx.Done():
		}
		return ctx.Err()
	}
	type dir struct{ path, rel string }
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	queue := []dir{{root, ""}}
	outstanding := 1 // directories taken from the queue or still on it, so an empty queue is not yet the end
	var n int64
	work := func() {
		for {
			mu.Lock()
			for len(queue) == 0 && outstanding > 0 {
				cond.Wait()
			}
			if len(queue) == 0 {
				mu.Unlock()
				return
			}
			d := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			mu.Unlock()

			ents, err := os.ReadDir(d.path)
			if err != nil {
				prog.warn(fmt.Sprintf("cannot read %s: %v", d.path, err))
			}
			var subs []dir
			for _, e := range ents {
				if ctx.Err() != nil {
					break
				}
				name := e.Name()
				r := name
				if d.rel != "" {
					r = d.rel + "/" + name
				}
				if excluded(r, globs) {
					continue
				}
				if e.IsDir() {
					subs = append(subs, dir{filepath.Join(d.path, name), r})
					continue
				}
				if e.Type()&os.ModeSymlink != 0 {
					continue
				}
				info, err := e.Info()
				if err != nil || !info.Mode().IsRegular() {
					continue
				}
				select {
				case out <- localFile{rel: r, size: info.Size()}:
				case <-ctx.Done():
				}
				if c := atomic.AddInt64(&n, 1); c%1000 == 0 {
					prog.note(fmt.Sprintf("indexing: %d files", c))
				}
			}
			mu.Lock()
			queue = append(queue, subs...)
			outstanding += len(subs) - 1 // this directory is finished, its children are not
			if outstanding == 0 {
				cond.Broadcast()
			} else {
				cond.Signal()
			}
			mu.Unlock()
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); work() }()
	}
	wg.Wait()
	return ctx.Err()
}

// index collects the whole tree at once, for the commands that have to hold it against something else entire.
func index(ctx context.Context, root string, globs []string, prog *progress) ([]localFile, error) {
	files := make(chan localFile, 4096)
	errc := make(chan error, 1)
	go func() { errc <- walkFiles(ctx, root, globs, prog, files) }()
	var out []localFile
	for f := range files {
		out = append(out, f)
	}
	if err := <-errc; err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].rel < out[j].rel })
	return out, ctx.Err()
}

// indexChunks hands the tree on in chunks of about n files, sorted within each chunk. The channel it writes to
// holds one chunk, so the walk runs a chunk ahead of the transfer and no further: what a push costs in memory
// stops depending on how large the study is.
func indexChunks(ctx context.Context, root string, globs []string, n int, prog *progress, out chan<- []localFile) error {
	defer close(out)
	files := make(chan localFile, 4096)
	errc := make(chan error, 1)
	go func() { errc <- walkFiles(ctx, root, globs, prog, files) }()
	buf := make([]localFile, 0, n)
	flush := func() bool {
		if len(buf) == 0 {
			return true
		}
		sort.Slice(buf, func(i, j int) bool { return buf[i].rel < buf[j].rel })
		select {
		case out <- buf:
			buf = make([]localFile, 0, n)
			return true
		case <-ctx.Done():
			return false
		}
	}
	for f := range files {
		buf = append(buf, f)
		if len(buf) < n {
			continue
		}
		if !flush() {
			break
		}
	}
	for range files { // if we stopped early, let the walk finish rather than leave it blocked on a send
	}
	if err := <-errc; err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !flush() {
		return ctx.Err()
	}
	return nil
}

// count walks the tree without keeping any of it, for the totals the progress line needs before the first byte
// moves. It costs a second pass over the directories, which is metadata only and leaves them warm in the cache
// for the walk that follows; without it a streaming push would not know what it was counting down to.
func count(ctx context.Context, root string, globs []string, prog *progress) (files, bytes int64, err error) {
	ch := make(chan localFile, 4096)
	errc := make(chan error, 1)
	go func() { errc <- walkFiles(ctx, root, globs, prog, ch) }()
	for f := range ch {
		files++
		bytes += f.size
	}
	if e := <-errc; e != nil {
		return 0, 0, e
	}
	return files, bytes, ctx.Err()
}

func sha256File(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ---- progress ---------------------------------------------------------------------------------------------------------------

type progress struct {
	jsonOut     bool
	mu          sync.Mutex
	start       time.Time
	bytes, done int64
	totalBytes  int64
	totalFiles  int64
	failed      int64
	last        string
	lastPrint   time.Time
	lastBytes   int64
	speed       float64
}

func newProgress(jsonOut bool) *progress { return &progress{jsonOut: jsonOut, start: time.Now()} }

// addTotals moves what the progress line counts down to. A streaming push starts from the whole tree and gives
// back what each plan reports the bridge already holds, so the figure is right from the first byte on a fresh
// push and settles onto the smaller one as a resume is planned.
func (p *progress) addTotals(files, bytes int64) {
	p.mu.Lock()
	p.totalFiles += files
	p.totalBytes += bytes
	p.mu.Unlock()
}
func (p *progress) add(bytes int64, files int64) {
	atomic.AddInt64(&p.bytes, bytes)
	atomic.AddInt64(&p.done, files)
	p.print(false)
}
func (p *progress) fail(rel string, err error) {
	atomic.AddInt64(&p.failed, 1)
	p.event(map[string]any{"event": "failed", "path": rel, "error": err.Error()})
	if !p.jsonOut {
		p.mu.Lock()
		fmt.Fprintf(os.Stderr, "\r\033[K  failed: %s: %v\n", rel, err)
		p.mu.Unlock()
	}
}
func (p *progress) warn(msg string) {
	p.event(map[string]any{"event": "warning", "message": msg})
	if !p.jsonOut {
		p.mu.Lock()
		fmt.Fprintf(os.Stderr, "\r\033[K  %s\n", msg)
		p.mu.Unlock()
	}
}
func (p *progress) note(msg string) {
	if p.jsonOut {
		return
	}
	p.mu.Lock()
	fmt.Fprintf(os.Stderr, "\r\033[K%s", msg)
	p.mu.Unlock()
}
func (p *progress) event(e map[string]any) {
	if !p.jsonOut {
		return
	}
	p.mu.Lock()
	b, _ := json.Marshal(e)
	fmt.Println(string(b))
	p.mu.Unlock()
}
func (p *progress) print(force bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if !force && now.Sub(p.lastPrint) < 200*time.Millisecond {
		return
	}
	b := atomic.LoadInt64(&p.bytes)
	if dt := now.Sub(p.lastPrint).Seconds(); dt > 0 && !p.lastPrint.IsZero() {
		inst := float64(b-p.lastBytes) / dt
		if p.speed == 0 {
			p.speed = inst
		} else {
			p.speed = 0.7*p.speed + 0.3*inst
		}
	}
	p.lastPrint, p.lastBytes = now, b
	if p.jsonOut {
		if force || now.Sub(p.start) > 0 {
			bb, _ := json.Marshal(map[string]any{"event": "progress", "bytes": b, "total_bytes": p.totalBytes, "files": atomic.LoadInt64(&p.done), "total_files": p.totalFiles, "bytes_per_second": int64(p.speed)})
			fmt.Println(string(bb))
		}
		return
	}
	pct := 0.0
	if p.totalBytes > 0 {
		pct = float64(b) / float64(p.totalBytes) * 100
	}
	eta := ""
	if p.speed > 1 && p.totalBytes > b {
		eta = fmt.Sprintf("  eta %s", (time.Duration(float64(p.totalBytes-b)/p.speed) * time.Second).Round(time.Second))
	}
	fmt.Fprintf(os.Stderr, "\r\033[K%5.1f%%  %s of %s  %d/%d files  %s/s%s", pct, fmtBytes(b), fmtBytes(p.totalBytes), atomic.LoadInt64(&p.done), p.totalFiles, fmtBytes(int64(p.speed)), eta)
}
func (p *progress) finish(msg string) {
	p.print(true)
	if !p.jsonOut {
		fmt.Fprintf(os.Stderr, "\r\033[K%s\n", msg)
	} else {
		p.event(map[string]any{"event": "done", "message": msg, "bytes": atomic.LoadInt64(&p.bytes), "files": atomic.LoadInt64(&p.done), "failed": atomic.LoadInt64(&p.failed), "seconds": time.Since(p.start).Seconds()})
	}
}

var busyMu sync.Mutex
var busyUntil time.Time

func busy(d time.Duration) {
	busyMu.Lock()
	busyUntil = time.Now().Add(d)
	busyMu.Unlock()
}

// ---- rate limit -------------------------------------------------------------------------------------------------------------

type limiter struct {
	rate   int64
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newLimiter(rate int64) *limiter {
	if rate <= 0 {
		return nil
	}
	return &limiter{rate: rate, tokens: float64(rate), last: time.Now()}
}
func (l *limiter) wait(ctx context.Context, n int) {
	if l == nil {
		return
	}
	for {
		l.mu.Lock()
		now := time.Now()
		l.tokens += now.Sub(l.last).Seconds() * float64(l.rate)
		if l.tokens > float64(l.rate) {
			l.tokens = float64(l.rate)
		}
		l.last = now
		if l.tokens >= float64(n) {
			l.tokens -= float64(n)
			l.mu.Unlock()
			return
		}
		need := time.Duration((float64(n) - l.tokens) / float64(l.rate) * float64(time.Second))
		l.mu.Unlock()
		select {
		case <-time.After(need):
		case <-ctx.Done():
			return
		}
	}
}

type limitedReader struct {
	r   io.Reader
	l   *limiter
	ctx context.Context
	cb  func(int)
}

func (x *limitedReader) Read(b []byte) (int, error) {
	if len(b) > 256*1024 {
		b = b[:256*1024]
	}
	n, err := x.r.Read(b)
	if n > 0 {
		x.l.wait(x.ctx, n)
		if x.cb != nil {
			x.cb(n)
		}
	}
	return n, err
}

// ---- push ----------------------------------------------------------------------------------------------------------------

type planResp struct {
	Have    []string `json:"have"`
	Missing []struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
	} `json:"missing"`
	Resumable []struct {
		Path      string `json:"path"`
		UploadID  string `json:"upload_id"`
		PartsDone []int  `json:"parts_done"`
	} `json:"resumable"`
	Limits Limits `json:"limits"`
}

// resume is what the bridge remembers of a large file whose upload was cut short.
type resume struct {
	id    string
	parts []int
}

// planFile is one entry of a plan request. It is a struct rather than a map because a plan asks about twenty
// thousand files at a time and a map per file was a real share of a large push's allocations.
type planFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Sha256 string `json:"sha256,omitempty"`
}

// planChunk asks the bridge which of these files it already holds and returns the ones still to send, with any
// large file it can resume and the size of what is already there. The map from remote path back to the file is
// built per request rather than over the whole tree: an answer only ever names paths from the request it
// answers, so a map across every file in the study bought nothing and cost a gigabyte on the large ones.
func planChunk(ctx context.Context, c *Client, files []localFile, remote func(string) string, prog *progress) ([]*localFile, map[string]resume, int64, error) {
	var missing []*localFile
	res := map[string]resume{}
	var have int64
	for i := 0; i < len(files); i += 20000 {
		end := i + 20000
		if end > len(files) {
			end = len(files)
		}
		slice := files[i:end]
		byRemote := make(map[string]*localFile, len(slice))
		req := struct {
			Files []planFile `json:"files"`
		}{Files: make([]planFile, 0, len(slice))}
		for j := range slice {
			p := remote(slice[j].rel)
			byRemote[p] = &slice[j]
			req.Files = append(req.Files, planFile{Path: p, Size: slice[j].size, Sha256: slice[j].sha})
		}
		var pr planResp
		if err := retry(ctx, 5, func() error { return c.json(ctx, "POST", "/api/plan", req, &pr) }); err != nil {
			return nil, nil, 0, fmt.Errorf("plan: %w", err)
		}
		for _, h := range pr.Have {
			if f := byRemote[h]; f != nil {
				have += f.size
			}
		}
		for _, m := range pr.Missing {
			if f := byRemote[m.Path]; f != nil {
				missing = append(missing, f)
			} else {
				prog.warn("plan returned a path we did not send: " + m.Path)
			}
		}
		for _, r := range pr.Resumable {
			res[r.Path] = resume{r.UploadID, r.PartsDone}
		}
	}
	return missing, res, have, nil
}

func cmdPush(ctx context.Context, args []string) error {
	o, err := parseOpts(args)
	if err != nil {
		return err
	}
	if len(o.positional) != 1 {
		return errors.New("usage: bifrost push <folder or file> [--to <path>] [options]")
	}
	src := o.positional[0]
	c, err := newClient()
	if err != nil {
		return err
	}
	sr, err := c.share(ctx)
	if err != nil {
		return err
	}
	if !sr.Share.CanUpload {
		return errors.New("this bridge does not receive files")
	}
	lim := sr.Share.Limits
	if o.workers > lim.Streams && lim.Streams > 0 {
		o.workers = lim.Streams
	}
	prog := newProgress(o.jsonOut)
	abs, _ := filepath.Abs(src)
	prefix := o.to
	if prefix == "" {
		if st, _ := os.Stat(abs); st != nil && st.IsDir() {
			prefix = filepath.Base(abs)
		}
	}
	prefix = strings.Trim(strings.ReplaceAll(prefix, "\\", "/"), "/")
	// Mirror the server's cleanPath: backslashes become separators, empty and "." segments drop out.
	// A name like `DICOM\I0` (written on Windows) lands as DICOM/I0 on both ends, so the plan
	// response keys match ours.
	cleanRel := func(rel string) string {
		parts := strings.Split(strings.ReplaceAll(rel, "\\", "/"), "/")
		out := parts[:0]
		for _, s := range parts {
			if s != "" && s != "." {
				out = append(out, s)
			}
		}
		return strings.Join(out, "/")
	}
	remote := func(rel string) string {
		rel = cleanRel(rel)
		if prefix == "" {
			return rel
		}
		return prefix + "/" + rel
	}
	// A push used to index the whole tree, then plan all of it, then send it: every path was held twice over,
	// with a map across all of them on top, so a study of a few million files cost gigabytes. That is what the
	// Synology's kernel killed a three day multiple_ms push for on 2026-09-05, at 6.9 GB on a 7.9 GB machine.
	// The tree is now counted once, then walked, planned and sent one chunk at a time, and the walk runs a
	// single chunk ahead of the transfer. What a push holds no longer follows how large the study is.
	const chunkFiles = 200_000
	// Go sizes its heap against how fast a program allocates, and walking several million paths allocates fast
	// even when almost none of it is kept, so resident memory drifted far above what was actually held. A soft
	// limit turns that drift into more frequent collection instead of more pages. It sits well above what a
	// chunk needs, so it costs nothing on an ordinary push, and GOMEMLIMIT still wins if an operator sets one.
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(1 << 30)
	}
	indexedFiles, indexedBytes, err := count(ctx, abs, o.excludes, prog)
	if err != nil {
		return err
	}
	if indexedFiles == 0 {
		return errors.New("nothing to send")
	}
	if !o.jsonOut {
		fmt.Fprintf(os.Stderr, "\r\033[K%d files indexed (%s); %d streams\n", indexedFiles, fmtBytes(indexedBytes), o.workers)
	}
	// The progress line counts down to the whole tree and is given back whatever each plan says the bridge
	// already holds, so a first push knows its total from the start and a resume settles onto the smaller one.
	prog.addTotals(indexedFiles, indexedBytes)

	walkCtx, stopWalk := context.WithCancel(ctx)
	defer stopWalk()
	chunks := make(chan []localFile, 1)
	walkErr := make(chan error, 1)
	go func() { walkErr <- indexChunks(walkCtx, abs, o.excludes, chunkFiles, prog, chunks) }()

	rl := newLimiter(o.limit)
	sem := make(streamSem, o.workers)
	jobs := make(chan func() error)
	poolErr := make(chan error, 1)
	go func() { poolErr <- runPool(ctx, o.workers, jobs) }()
	send := func(j func() error) bool {
		select {
		case jobs <- j:
			return true
		case <-ctx.Done():
			return false
		}
	}

	var alreadyThere, toSend, sendBytes, haveBytes int64
	var planErr error
	for chunk := range chunks {
		if o.checksum {
			prog.note("hashing local files")
			hashAll(ctx, abs, chunk, 8)
		}
		missing, resumable, have, err := planChunk(ctx, c, chunk, remote, prog)
		if err != nil {
			planErr = err
			break
		}
		var chunkBytes int64
		for _, f := range missing {
			chunkBytes += f.size
		}
		present := int64(len(chunk) - len(missing))
		alreadyThere += present
		toSend += int64(len(missing))
		sendBytes += chunkBytes
		haveBytes += have
		prog.addTotals(-present, -have)
		prog.event(map[string]any{"event": "plan", "files": len(chunk), "already_there": present, "to_send": len(missing), "bytes": chunkBytes})
		if o.dryRun {
			for _, f := range missing {
				fmt.Println(remote(f.rel), f.size)
			}
			continue
		}
		// Large files first within the chunk, because they take the longest; the batches behind them keep the
		// rest of the streams busy while they run.
		var small []*localFile
		stop := false
		for _, f := range missing {
			if f.size < lim.LargeFile {
				small = append(small, f)
				continue
			}
			f, r := f, resumable[remote(f.rel)]
			if !send(func() error {
				return pushLarge(ctx, c, abs, f, remote(f.rel), lim, r.id, r.parts, rl, prog, o.workers, sem)
			}) {
				stop = true
				break
			}
		}
		if !stop {
			for _, b := range makeBatches(small, lim.BatchBytes, lim.BatchFiles) {
				b := b
				if !send(func() error { return pushBatch(ctx, c, abs, b, remote, rl, prog, sem) }) {
					stop = true
					break
				}
			}
		}
		if stop {
			break
		}
	}
	// Whatever ended the loop, the walk is told to stop and drained: leaving it blocked on a send would leave
	// its error unread and this function waiting on it forever.
	stopWalk()
	for range chunks {
	}
	<-walkErr
	close(jobs)
	err = <-poolErr
	if planErr != nil {
		return planErr
	}
	if o.dryRun {
		return nil
	}
	if toSend == 0 {
		prog.finish("nothing to send; everything is already on the bridge")
		return nil
	}
	if !o.jsonOut {
		// What the plans came back with, not what went over the wire: an interrupted push has planned more
		// than it sent, and the closing line below is the one that says how much actually arrived.
		fmt.Fprintf(os.Stderr, "\r\033[Kplanned: %d already on the bridge (%s), %d to send (%s)\n", alreadyThere, fmtBytes(haveBytes), toSend, fmtBytes(sendBytes))
	}
	failed := atomic.LoadInt64(&prog.failed)
	_ = c.json(ctx, "POST", "/api/upload/done", map[string]any{"files": atomic.LoadInt64(&prog.done), "bytes": atomic.LoadInt64(&prog.bytes)}, nil)
	if err != nil && ctx.Err() != nil {
		return err
	}
	if failed > 0 {
		prog.finish(fmt.Sprintf("sent %d files (%s); %d failed. Run the same command again to send the rest.", atomic.LoadInt64(&prog.done), fmtBytes(atomic.LoadInt64(&prog.bytes)), failed))
		return fmt.Errorf("transfer finished with %d failed files", failed)
	}
	prog.finish(fmt.Sprintf("sent %d files (%s) in %s, every file verified", atomic.LoadInt64(&prog.done), fmtBytes(atomic.LoadInt64(&prog.bytes)), time.Since(prog.start).Round(time.Second)))
	return nil
}

func hashAll(ctx context.Context, root string, files []localFile, workers int) {
	var wg sync.WaitGroup
	ch := make(chan int)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				if s, err := sha256File(files[i].abs(root)); err == nil {
					files[i].sha = s
				}
			}
		}()
	}
	for i := range files {
		if ctx.Err() != nil {
			break
		}
		ch <- i
	}
	close(ch)
	wg.Wait()
}

func makeBatches(files []*localFile, maxBytes int64, maxFiles int) [][]*localFile {
	var out [][]*localFile
	var cur []*localFile
	var n int64
	for _, f := range files {
		if len(cur) > 0 && (n+f.size > maxBytes || len(cur) >= maxFiles) {
			out = append(out, cur)
			cur, n = nil, 0
		}
		cur = append(cur, f)
		n += f.size
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// streamSem bounds the transfer streams of one push to --workers, batches and large-file parts together, so a
// push never holds more streams on the bridge than it was told to use. Without it every pool worker that met a
// large file opened workers/2 part streams of its own: 24 workers could ask for far more than the bridge's
// budget, and the 503 hold-offs that followed stalled every client on the bridge (2026-09-02).
type streamSem chan struct{}

func (s streamSem) acquire(ctx context.Context) error {
	select {
	case s <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s streamSem) release() { <-s }

// runPool runs jobs taken from ch on a fixed number of workers, until ch is closed. The caller owns ch and
// closes it, which is what lets a push feed the pool as it walks instead of building every job in advance.
func runPool(ctx context.Context, workers int, ch <-chan func() error) error {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range ch {
				// Respect a server "busy" hold-off before starting new work.
				for {
					busyMu.Lock()
					until := busyUntil
					busyMu.Unlock()
					if time.Now().After(until) {
						break
					}
					select {
					case <-time.After(time.Until(until)):
					case <-ctx.Done():
						return
					}
				}
				_ = j()
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// runPoolSlice feeds a list of jobs that is already complete through the pool.
func runPoolSlice(ctx context.Context, workers int, jobs []func() error) error {
	ch := make(chan func() error)
	go func() {
		defer close(ch)
		for _, j := range jobs {
			select {
			case ch <- j:
			case <-ctx.Done():
				return
			}
		}
	}()
	return runPool(ctx, workers, ch)
}

// pushBatch streams one tar.zst of small files; the server answers with what it wrote and its hashes.
func pushBatch(ctx context.Context, c *Client, root string, files []*localFile, remote func(string) string, rl *limiter, prog *progress, sem streamSem) error {
	var bytesDeclared int64
	for _, f := range files {
		bytesDeclared += f.size
	}
	local := map[string]string{}
	var lmu sync.Mutex
	err := retry(ctx, 5, func() error {
		if err := sem.acquire(ctx); err != nil {
			return err
		}
		defer sem.release()
		pr, pw := io.Pipe()
		go func() {
			zw, _ := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(2))
			tw := tar.NewWriter(zw)
			var werr error
			for _, f := range files {
				if ctx.Err() != nil {
					werr = ctx.Err()
					break
				}
				fh, err := os.Open(f.abs(root))
				if err != nil {
					prog.fail(f.rel, err)
					continue
				}
				st, _ := fh.Stat()
				if err := tw.WriteHeader(&tar.Header{Name: remote(f.rel), Size: st.Size(), Mode: 0o644, ModTime: st.ModTime(), Typeflag: tar.TypeReg}); err != nil {
					fh.Close()
					werr = err
					break
				}
				h := sha256.New()
				var rd io.Reader = io.TeeReader(io.LimitReader(fh, st.Size()), h)
				if rl != nil {
					rd = &limitedReader{r: rd, l: rl, ctx: ctx}
				}
				if _, err := io.Copy(tw, rd); err != nil {
					fh.Close()
					werr = err
					break
				}
				fh.Close()
				lmu.Lock()
				local[remote(f.rel)] = hex.EncodeToString(h.Sum(nil))
				lmu.Unlock()
			}
			if werr == nil {
				werr = tw.Close()
			}
			if werr == nil {
				werr = zw.Close()
			}
			pw.CloseWithError(werr)
		}()
		res, err := c.req(ctx, "POST", "/api/upload/batch", pr, map[string]string{"Content-Type": "application/zstd", "Content-Encoding": "zstd", "X-Batch-Bytes": fmt.Sprint(bytesDeclared), "X-Batch-Files": fmt.Sprint(len(files))})
		pr.Close()
		if err != nil {
			return err
		}
		defer res.Body.Close()
		var out struct {
			Files []struct {
				Path   string `json:"path"`
				Size   int64  `json:"size"`
				Sha256 string `json:"sha256"`
			} `json:"files"`
		}
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			return err
		}
		got := map[string]string{}
		for _, f := range out.Files {
			got[f.Path] = f.Sha256
		}
		var bad []string
		var okBytes, okFiles int64
		for _, f := range files {
			rp := remote(f.rel)
			if got[rp] != "" && got[rp] == local[rp] {
				okBytes += f.size
				okFiles++
			} else {
				bad = append(bad, rp)
			}
		}
		prog.add(okBytes, okFiles)
		if len(bad) > 0 {
			return fmt.Errorf("%d files did not verify", len(bad))
		}
		return nil
	})
	if err != nil {
		for _, f := range files {
			prog.fail(f.rel, err)
		}
	}
	return err
}

// pushLarge sends one big file as parts, several in flight, then asks the server to verify and place it.
func pushLarge(ctx context.Context, c *Client, root string, f *localFile, rpath string, lim Limits, resumeID string, done []int, rl *limiter, prog *progress, workers int, sem streamSem) error {
	if f.sha == "" {
		prog.note("hashing " + f.rel)
		s, err := sha256File(f.abs(root))
		if err != nil {
			prog.fail(f.rel, err)
			return err
		}
		f.sha = s
	}
	var init struct {
		Already   bool   `json:"already"`
		UploadID  string `json:"upload_id"`
		PartSize  int64  `json:"part_size"`
		Total     int    `json:"parts_total"`
		PartsDone []int  `json:"parts_done"`
	}
	if err := retry(ctx, 5, func() error {
		return c.json(ctx, "POST", "/api/upload/init", map[string]any{"path": rpath, "size": f.size, "sha256": f.sha}, &init)
	}); err != nil {
		prog.fail(f.rel, err)
		return err
	}
	if init.Already {
		prog.add(f.size, 1)
		return nil
	}
	doneSet := map[int]bool{}
	for _, p := range init.PartsDone {
		doneSet[p] = true
	}
	var pending []int
	for i := 0; i < init.Total; i++ {
		if doneSet[i] {
			prog.add(min64(init.PartSize, f.size-int64(i)*init.PartSize), 0)
		} else {
			pending = append(pending, i)
		}
	}
	inflight := workers / 2
	if inflight < 2 {
		inflight = 2
	}
	var failed atomic.Int64
	var wg sync.WaitGroup
	ch := make(chan int)
	for i := 0; i < inflight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range ch {
				off := int64(n) * init.PartSize
				size := min64(init.PartSize, f.size-off)
				err := retry(ctx, 6, func() error {
					if err := sem.acquire(ctx); err != nil {
						return err
					}
					defer sem.release()
					fh, err := os.Open(f.abs(root))
					if err != nil {
						return err
					}
					defer fh.Close()
					sec := io.NewSectionReader(fh, off, size)
					h := sha256.New()
					if _, err := io.Copy(h, sec); err != nil {
						return err
					}
					sec.Seek(0, io.SeekStart)
					var rd io.Reader = sec
					var sent int64
					rd = &limitedReader{r: rd, l: rl, ctx: ctx, cb: func(k int) { sent += int64(k); prog.add(int64(k), 0) }}
					res, err := c.req(ctx, "PUT", fmt.Sprintf("/api/upload/%s/part/%d", init.UploadID, n), rd, map[string]string{"Content-Type": "application/octet-stream", "Content-Length": fmt.Sprint(size), "X-Part-Sha256": hex.EncodeToString(h.Sum(nil))})
					if err != nil {
						atomic.AddInt64(&prog.bytes, -sent) // undo the progress of a failed part
						return err
					}
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
					return nil
				})
				if err != nil {
					failed.Add(1)
				}
			}
		}()
	}
	for _, n := range pending {
		if ctx.Err() != nil {
			break
		}
		select {
		case ch <- n:
		case <-ctx.Done():
		}
	}
	close(ch)
	wg.Wait()
	if failed.Load() > 0 || ctx.Err() != nil {
		err := fmt.Errorf("%d parts failed", failed.Load())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		prog.fail(f.rel, err)
		return err
	}
	if err := retry(ctx, 5, func() error {
		return c.json(ctx, "POST", fmt.Sprintf("/api/upload/%s/complete", init.UploadID), map[string]any{}, nil)
	}); err != nil {
		prog.fail(f.rel, err)
		return err
	}
	prog.add(0, 1)
	return nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ---- pull ----------------------------------------------------------------------------------------------------------------

func cmdPull(ctx context.Context, args []string) error {
	o, err := parseOpts(args)
	if err != nil {
		return err
	}
	if len(o.positional) != 1 {
		return errors.New("usage: bifrost pull <folder> [--from <path>] [options]")
	}
	dest := o.positional[0]
	c, err := newClient()
	if err != nil {
		return err
	}
	sr, err := c.share(ctx)
	if err != nil {
		return err
	}
	if !sr.Share.CanDownload {
		return errors.New("this bridge has nothing to download")
	}
	lim := sr.Share.Limits
	if o.workers > lim.Streams && lim.Streams > 0 {
		o.workers = lim.Streams
	}
	prog := newProgress(o.jsonOut)
	var man struct {
		Files []Entry `json:"files"`
	}
	if err := retry(ctx, 5, func() error {
		return c.json(ctx, "GET", "/api/manifest?box=out&path="+url.QueryEscape(o.from), nil, &man)
	}); err != nil {
		return err
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	strip := strings.Trim(o.from, "/")
	localPath := func(rp string) string {
		rel := rp
		if strip != "" {
			rel = strings.TrimPrefix(strings.TrimPrefix(rp, strip), "/")
		}
		return filepath.Join(dest, filepath.FromSlash(rel))
	}
	var need []Entry
	var have int
	var total int64
	for _, e := range man.Files {
		if excluded(e.Path, o.excludes) {
			continue
		}
		lp := localPath(e.Path)
		if st, err := os.Stat(lp); err == nil && st.Size() == e.Size {
			if !o.checksum || e.Sha256 == nil {
				have++
				continue
			}
			if s, _ := sha256File(lp); s == *e.Sha256 {
				have++
				continue
			}
		}
		need = append(need, e)
		total += e.Size
	}
	prog.totalBytes, prog.totalFiles = total, int64(len(need))
	prog.event(map[string]any{"event": "plan", "files": len(man.Files), "already_here": have, "to_fetch": len(need), "bytes": total})
	if !o.jsonOut {
		fmt.Fprintf(os.Stderr, "%d files on the bridge; %d already here; %d to fetch (%s); %d streams\n", len(man.Files), have, len(need), fmtBytes(total), o.workers)
	}
	if o.dryRun {
		for _, e := range need {
			fmt.Println(e.Path, e.Size)
		}
		return nil
	}
	if len(need) == 0 {
		prog.finish("nothing to fetch; everything is already here")
		return nil
	}
	rl := newLimiter(o.limit)
	var large, small []Entry
	for _, e := range need {
		if e.Size >= lim.LargeFile {
			large = append(large, e)
		} else {
			small = append(small, e)
		}
	}
	var jobs []func() error
	var cur []Entry
	var n int64
	flush := func() {
		if len(cur) == 0 {
			return
		}
		b := cur
		jobs = append(jobs, func() error { return pullBatch(ctx, c, b, localPath, rl, prog) })
		cur, n = nil, 0
	}
	for _, e := range small {
		if len(cur) > 0 && (n+e.Size > lim.BatchBytes || len(cur) >= lim.BatchFiles) {
			flush()
		}
		cur = append(cur, e)
		n += e.Size
	}
	flush()
	for _, e := range large {
		e := e
		jobs = append(jobs, func() error { return pullLarge(ctx, c, e, localPath(e.Path), lim, rl, prog, o.workers) })
	}
	err = runPoolSlice(ctx, o.workers, jobs)
	failed := atomic.LoadInt64(&prog.failed)
	_ = c.json(ctx, "POST", "/api/download/done", map[string]any{"files": atomic.LoadInt64(&prog.done), "bytes": atomic.LoadInt64(&prog.bytes)}, nil)
	if err != nil && ctx.Err() != nil {
		return err
	}
	if failed > 0 {
		prog.finish(fmt.Sprintf("fetched %d files (%s); %d failed. Run the same command again to fetch the rest.", atomic.LoadInt64(&prog.done), fmtBytes(atomic.LoadInt64(&prog.bytes)), failed))
		return fmt.Errorf("transfer finished with %d failed files", failed)
	}
	prog.finish(fmt.Sprintf("fetched %d files (%s) in %s, every file verified", atomic.LoadInt64(&prog.done), fmtBytes(atomic.LoadInt64(&prog.bytes)), time.Since(prog.start).Round(time.Second)))
	return nil
}

func pullBatch(ctx context.Context, c *Client, files []Entry, localPath func(string) string, rl *limiter, prog *progress) error {
	want := map[string]Entry{}
	for _, e := range files {
		want[e.Path] = e
	}
	err := retry(ctx, 5, func() error {
		paths := make([]string, 0, len(files))
		for _, e := range files {
			paths = append(paths, e.Path)
		}
		res, err := c.req(ctx, "POST", "/api/download/batch?box=out", jsonBody(map[string]any{"paths": paths, "zstd": true}), map[string]string{"Content-Type": "application/json"})
		if err != nil {
			return err
		}
		defer res.Body.Close()
		zr, err := zstd.NewReader(res.Body)
		if err != nil {
			return err
		}
		defer zr.Close()
		tr := tar.NewReader(zr)
		got := map[string]bool{}
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			e, ok := want[h.Name]
			if !ok || h.Typeflag != tar.TypeReg {
				io.Copy(io.Discard, tr)
				continue
			}
			lp := localPath(h.Name)
			var rd io.Reader = tr
			if rl != nil {
				rd = &limitedReader{r: rd, l: rl, ctx: ctx}
			}
			sum, size, err := writeVerified(lp, rd, h.ModTime)
			if err != nil {
				return err
			}
			if size != e.Size || (e.Sha256 != nil && *e.Sha256 != sum) {
				os.Remove(lp)
				return fmt.Errorf("%s did not verify", h.Name)
			}
			got[h.Name] = true
			prog.add(size, 1)
		}
		for _, e := range files {
			if !got[e.Path] {
				return fmt.Errorf("%s missing from batch", e.Path)
			}
		}
		return nil
	})
	if err != nil {
		for _, e := range files {
			prog.fail(e.Path, err)
		}
	}
	return err
}

func writeVerified(lp string, rd io.Reader, mtime time.Time) (string, int64, error) {
	if err := os.MkdirAll(filepath.Dir(lp), 0o755); err != nil {
		return "", 0, err
	}
	tmp := lp + ".bifrost-tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", 0, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), rd)
	f.Close()
	if err != nil {
		os.Remove(tmp)
		return "", 0, err
	}
	if !mtime.IsZero() {
		os.Chtimes(tmp, mtime, mtime)
	}
	if err := os.Rename(tmp, lp); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func pullLarge(ctx context.Context, c *Client, e Entry, lp string, lim Limits, rl *limiter, prog *progress, workers int) error {
	if err := os.MkdirAll(filepath.Dir(lp), 0o755); err != nil {
		prog.fail(e.Path, err)
		return err
	}
	tmp := lp + ".bifrost-part"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		prog.fail(e.Path, err)
		return err
	}
	if err := f.Truncate(e.Size); err != nil {
		f.Close()
		prog.fail(e.Path, err)
		return err
	}
	// Resume: a sidecar lists the parts already fetched.
	doneFile := tmp + ".done"
	done := map[int]bool{}
	if b, err := os.ReadFile(doneFile); err == nil {
		for _, s := range strings.Fields(string(b)) {
			var n int
			fmt.Sscanf(s, "%d", &n)
			done[n] = true
		}
	}
	total := int((e.Size + lim.PartSize - 1) / lim.PartSize)
	if total == 0 {
		total = 1
	}
	var dmu sync.Mutex
	markDone := func(n int) {
		dmu.Lock()
		done[n] = true
		var sb strings.Builder
		for k := range done {
			fmt.Fprintf(&sb, "%d ", k)
		}
		os.WriteFile(doneFile, []byte(sb.String()), 0o644)
		dmu.Unlock()
	}
	var pending []int
	for i := 0; i < total; i++ {
		if done[i] {
			prog.add(min64(lim.PartSize, e.Size-int64(i)*lim.PartSize), 0)
		} else {
			pending = append(pending, i)
		}
	}
	inflight := workers / 2
	if inflight < 2 {
		inflight = 2
	}
	var failed atomic.Int64
	var wg sync.WaitGroup
	ch := make(chan int)
	for i := 0; i < inflight; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range ch {
				off := int64(n) * lim.PartSize
				size := min64(lim.PartSize, e.Size-off)
				err := retry(ctx, 6, func() error {
					res, err := c.req(ctx, "GET", "/api/download?box=out&path="+url.QueryEscape(e.Path), nil, map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", off, off+size-1)})
					if err != nil {
						return err
					}
					defer res.Body.Close()
					if res.StatusCode != http.StatusPartialContent {
						return fmt.Errorf("expected a partial response, got %d", res.StatusCode)
					}
					var got int64
					rd := &limitedReader{r: res.Body, l: rl, ctx: ctx, cb: func(k int) { got += int64(k); prog.add(int64(k), 0) }}
					w := io.NewOffsetWriter(f, off)
					n2, err := io.Copy(w, rd)
					if err != nil {
						atomic.AddInt64(&prog.bytes, -got)
						return err
					}
					if n2 != size {
						atomic.AddInt64(&prog.bytes, -got)
						return fmt.Errorf("short part: %d of %d", n2, size)
					}
					return nil
				})
				if err != nil {
					failed.Add(1)
				} else {
					markDone(n)
				}
			}
		}()
	}
	for _, n := range pending {
		if ctx.Err() != nil {
			break
		}
		select {
		case ch <- n:
		case <-ctx.Done():
		}
	}
	close(ch)
	wg.Wait()
	f.Close()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if failed.Load() > 0 {
		err := fmt.Errorf("%d parts failed", failed.Load())
		prog.fail(e.Path, err)
		return err
	}
	if e.Sha256 != nil {
		sum, err := sha256File(tmp)
		if err != nil || sum != *e.Sha256 {
			os.Remove(tmp)
			os.Remove(doneFile)
			err = fmt.Errorf("checksum mismatch after download")
			prog.fail(e.Path, err)
			return err
		}
	}
	if st, err := os.Stat(tmp); err == nil && st.Size() != e.Size {
		os.Remove(tmp)
		os.Remove(doneFile)
		err = fmt.Errorf("size mismatch after download")
		prog.fail(e.Path, err)
		return err
	}
	if t, err := time.Parse(time.RFC3339Nano, e.Mtime); err == nil {
		os.Chtimes(tmp, t, t)
	}
	os.Remove(doneFile)
	if err := os.Rename(tmp, lp); err != nil {
		prog.fail(e.Path, err)
		return err
	}
	prog.add(0, 1)
	return nil
}

func jsonBody(v any) io.Reader {
	b, _ := json.Marshal(v)
	return strings.NewReader(string(b))
}

// ---- other commands ------------------------------------------------------------------------------------------------------------

func cmdLogin(args []string) error {
	cfg := loadConfig()
	tok := ""
	for _, a := range args {
		if strings.HasPrefix(a, "--url=") {
			cfg.URL = strings.TrimSuffix(strings.TrimPrefix(a, "--url="), "/")
		} else if strings.HasPrefix(a, "http") && strings.Contains(a, "#bfr_") {
			cfg.URL = strings.TrimSuffix(strings.Split(a, "#")[0], "/")
			tok = strings.Split(a, "#")[1]
		} else if strings.HasPrefix(a, "bfr_") {
			tok = a
		}
	}
	if tok == "" {
		fmt.Fprint(os.Stderr, "token: ")
		fmt.Scanln(&tok)
	}
	cfg.Token = strings.TrimSpace(tok)
	cfg.Passcode = os.Getenv("BIFROST_PASSCODE")
	c := &Client{cfg: cfg, http: http.DefaultClient}
	sr, err := c.share(mainCtx)
	var ae *apiError
	if errors.As(err, &ae) && ae.Passcode {
		fmt.Fprint(os.Stderr, "this bridge has a passcode: ")
		var pc string
		fmt.Scanln(&pc)
		cfg.Passcode = strings.TrimSpace(pc)
		c.cfg = cfg
		sr, err = c.share(mainCtx)
	}
	if err != nil {
		return err
	}
	if err := saveConfig(cfg); err != nil {
		return err
	}
	printBridge(sr.Share, sr.Credential.Label, cfg.URL)
	fmt.Printf("\ntoken saved in %s. Next: bifrost push <folder>  or  bifrost pull <folder>\n", configPath())
	return nil
}

// printBridge shows what this token opens: name, partner, what it allows, limits, closing date, message.
func printBridge(s Share, label, url string) {
	line := strings.Repeat("─", 64)
	fmt.Println(line)
	fmt.Printf("  %s\n", s.Name)
	if s.Partner != "" {
		fmt.Printf("  for %s", s.Partner)
		if label != "" {
			fmt.Printf(" (token: %s)", label)
		}
		fmt.Println()
	} else if label != "" {
		fmt.Printf("  token: %s\n", label)
	}
	fmt.Println(line)
	switch s.Direction {
	case "both":
		fmt.Println("  you can        send files to kineuro and download files prepared for you")
	case "in":
		fmt.Println("  you can        send files to kineuro (no download)")
	default:
		fmt.Println("  you can        download the files prepared for you (no upload)")
	}
	st := s.Status
	if st == "open" && s.ExpiresAt != nil {
		if t, err := time.Parse(time.RFC3339, *s.ExpiresAt); err == nil {
			d := int(time.Until(t).Hours() / 24)
			if d < 0 {
				st = "expired"
			} else {
				st = fmt.Sprintf("open, closes %s (%d days left)", (*s.ExpiresAt)[:10], d)
			}
		}
	} else if st == "open" {
		st = "open, no closing date"
	}
	fmt.Printf("  status         %s\n", st)
	if s.Direction != "out" {
		q := "no limit"
		if s.QuotaBytes > 0 {
			q = fmt.Sprintf("%s of %s used", fmtBytes(s.UsedBytes), fmtBytes(s.QuotaBytes))
		} else if s.UsedBytes > 0 {
			q = fmt.Sprintf("%s used, no limit", fmtBytes(s.UsedBytes))
		}
		fmt.Printf("  space          %s\n", q)
		if s.MaxFiles > 0 {
			fmt.Printf("  files          %d of %d\n", s.Files, s.MaxFiles)
		} else if s.Files > 0 {
			fmt.Printf("  files          %d received so far\n", s.Files)
		}
	}
	if s.Direction != "in" && s.MaxDownloadBytes > 0 {
		fmt.Printf("  download       %s of %s allowance used\n", fmtBytes(s.DownloadedBytes), fmtBytes(s.MaxDownloadBytes))
	}
	if s.Message != "" {
		fmt.Printf("  message        %s\n", s.Message)
	}
	fmt.Printf("  bridge         %s (%s)\n", url, s.ID)
	fmt.Println(line)
}
func cmdLogout() error { os.Remove(configPath()); fmt.Println("token forgotten"); return nil }

func describe(s Share) string {
	var parts []string
	switch s.Direction {
	case "both":
		parts = append(parts, "you can push and pull")
	case "in":
		parts = append(parts, "you can push")
	default:
		parts = append(parts, "you can pull")
	}
	if s.ExpiresAt != nil {
		parts = append(parts, "it closes on "+(*s.ExpiresAt)[:10])
	}
	if s.QuotaBytes > 0 {
		parts = append(parts, fmt.Sprintf("%s of %s used", fmtBytes(s.UsedBytes), fmtBytes(s.QuotaBytes)))
	}
	return strings.Join(parts, "; ") + "."
}
func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func cmdStatus() error {
	c, err := newClient()
	if err != nil {
		return err
	}
	sr, err := c.share(mainCtx)
	if err != nil {
		return err
	}
	printBridge(sr.Share, sr.Credential.Label, c.cfg.URL)
	fmt.Printf("  transfer       parts of %s for large files, batches up to %s or %d files, %d parallel streams\n", fmtBytes(sr.Share.Limits.PartSize), fmtBytes(sr.Share.Limits.BatchBytes), sr.Share.Limits.BatchFiles, sr.Share.Limits.Streams)
	return nil
}

func cmdLs(args []string) error {
	c, err := newClient()
	if err != nil {
		return err
	}
	p := ""
	box := ""
	for _, a := range args {
		if a == "--sent" {
			box = "in"
		} else {
			p = a
		}
	}
	sr, err := c.share(mainCtx)
	if err != nil {
		return err
	}
	if box == "" {
		if sr.Share.CanDownload {
			box = "out"
		} else {
			box = "in"
		}
	}
	var out struct {
		Entries []Entry `json:"entries"`
	}
	ep := "/api/ls?box=" + box + "&path=" + url.QueryEscape(p)
	if box == "in" {
		ep = "/api/sent?path=" + url.QueryEscape(p)
	}
	if err := c.json(mainCtx, "GET", ep, nil, &out); err != nil {
		return err
	}
	for _, e := range out.Entries {
		if e.Dir {
			fmt.Printf("%12s  %s/\n", "", e.Path)
		} else {
			fmt.Printf("%12s  %s\n", fmtBytes(e.Size), e.Path)
		}
	}
	if len(out.Entries) == 0 {
		fmt.Println("(empty)")
	}
	return nil
}

func cmdVerify(ctx context.Context, args []string) error {
	o, err := parseOpts(args)
	if err != nil {
		return err
	}
	if len(o.positional) != 1 {
		return errors.New("usage: bifrost verify <folder> [--to <path> | --from <path>]")
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	sr, err := c.share(ctx)
	if err != nil {
		return err
	}
	box := "in"
	prefix := o.to
	if !sr.Share.CanUpload || o.from != "" {
		box = "out"
		prefix = o.from
	}
	abs, _ := filepath.Abs(o.positional[0])
	if prefix == "" && box == "in" {
		if st, _ := os.Stat(abs); st != nil && st.IsDir() {
			prefix = filepath.Base(abs)
		}
	}
	prog := newProgress(o.jsonOut)
	files, err := index(ctx, abs, o.excludes, prog)
	if err != nil {
		return err
	}
	prog.note("hashing local files")
	hashAll(ctx, abs, files, 8)
	var man struct {
		Files []Entry `json:"files"`
	}
	if err := retry(ctx, 5, func() error {
		return c.json(ctx, "GET", "/api/manifest?box="+box+"&path="+url.QueryEscape(prefix), nil, &man)
	}); err != nil {
		return err
	}
	remote := map[string]Entry{}
	for _, e := range man.Files {
		remote[e.Path] = e
	}
	var ok, bad, missing int
	for _, f := range files {
		rp := f.rel
		if prefix != "" {
			rp = strings.Trim(prefix, "/") + "/" + f.rel
		}
		e, found := remote[rp]
		switch {
		case !found:
			missing++
			fmt.Println("missing on bridge:", rp)
		case e.Size != f.size || (e.Sha256 != nil && *e.Sha256 != f.sha):
			bad++
			fmt.Println("differs:", rp)
		default:
			ok++
		}
	}
	fmt.Fprintf(os.Stderr, "\r\033[K%d verified, %d differ, %d missing on the bridge, %d on the bridge only\n", ok, bad, missing, len(man.Files)-ok-bad)
	if bad > 0 || missing > 0 {
		return fmt.Errorf("transfer finished with %d files not matching", bad+missing)
	}
	return nil
}

func cmdUpdate() error {
	cfg := loadConfig()
	name := fmt.Sprintf("bifrost-%s-%s", runtimeOS(), runtimeArch())
	if runtimeOS() == "windows" {
		name += ".exe"
	}
	// The newest version lives on GitHub Releases; the bridge itself is the fallback (it serves what it was deployed with).
	latest, base := "", ""
	if res, err := http.Get("https://api.github.com/repos/kineuro/bifrost/releases/latest"); err == nil {
		var r struct {
			Tag string `json:"tag_name"`
		}
		if json.NewDecoder(res.Body).Decode(&r) == nil && r.Tag != "" {
			latest, base = strings.TrimPrefix(r.Tag, "v"), "https://github.com/kineuro/bifrost/releases/download/"+r.Tag+"/"
		}
		res.Body.Close()
	}
	if latest == "" {
		res, err := http.Get(cfg.URL + "/api/cli")
		if err != nil {
			return err
		}
		defer res.Body.Close()
		var info struct {
			Version string `json:"version"`
			Base    string `json:"base"`
		}
		if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
			return err
		}
		latest, base = info.Version, info.Base
	}
	if latest == version || strings.HasPrefix(version, latest+"+") {
		fmt.Println("already up to date:", version)
		return nil
	}
	fmt.Printf("updating %s -> %s\n", version, latest)
	self, err := os.Executable()
	if err != nil {
		return err
	}
	r2, err := http.Get(base + name)
	if err != nil {
		return err
	}
	defer r2.Body.Close()
	if r2.StatusCode != 200 {
		return fmt.Errorf("no build for %s at %s", name, base)
	}
	tmp := self + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r2.Body); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if runtimeOS() == "windows" {
		os.Rename(self, self+".old")
	}
	if err := os.Rename(tmp, self); err != nil {
		return err
	}
	fmt.Println("updated to", latest)
	return nil
}
