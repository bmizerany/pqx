package fetch

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/xi2/xz"
	"kr.dev/errorfmt"
)

var envCacheDir = os.Getenv("PQX_BIN_DIR")

func BinaryURL(version string) string {
	const fetchURLTempl = "https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-$OS-$ARCH/$VERSION/embedded-postgres-binaries-$OS-$ARCH-$VERSION.jar"

	// TODO(bmizerany): validate version
	return strings.NewReplacer(
		"$OS", getOS(),
		"$ARCH", getArch(),
		"$VERSION", version,
	).Replace(fetchURLTempl)
}

func Binary(ctx context.Context, version string) (binDir string, err error) {
	defer errorfmt.Handlef("fetchBinary: %w", &err)

	cacheDir := envCacheDir
	var pgDir string
	if cacheDir == "" {
		var err error
		cacheDir, err = os.UserHomeDir()
		if err != nil {
			return "", err
		}
		pgDir, err = filepath.Abs(filepath.Join(cacheDir, ".cache/pqx", version))
		if err != nil {
			return "", err
		}
	}

	if err := os.MkdirAll(pgDir, 0755); err != nil {
		return "", err
	}

	binDir = path.Join(pgDir, "bin")
	_, err = os.Stat(binDir)
	if err == nil {
		// already cached
		// TODO(bmizerany): validate the dir has what we think it has?
		return binDir, nil
	}

	binURL := BinaryURL(version)
	defer errorfmt.Handlef("%s: %w", binURL, &err)

	req, err := http.NewRequestWithContext(ctx, "GET", binURL, nil)
	if err != nil {
		return "", err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if err := extractJar(ctx, pgDir, res.Body); err != nil {
		return "", err
	}

	return binDir, nil
}

func extractJar(ctx context.Context, dir string, r io.Reader) (err error) {
	defer errorfmt.Handlef("extractJar: %w", &err)

	buf, size, err := slurp(r)
	if err != nil {
		return err
	}

	zr, err := zip.NewReader(buf, size)
	if err != nil {
		return err
	}

	for _, f := range zr.File {
		matched, err := path.Match("postgres-*.txz", f.Name)
		if err != nil {
			return err
		}
		if !matched {
			continue
		}

		o, err := f.Open()
		if err != nil {
			return err
		}
		defer o.Close()
		return extractTxn(ctx, dir, o)
	}

	return errors.New("no postgres-*.txz found in archive")
}

func extractTxn(ctx context.Context, dir string, r io.Reader) (err error) {
	defer errorfmt.Handlef("extractTxn: %w", &err)

	xr, err := xz.NewReader(r, 0)
	if err != nil {
		return err
	}
	tr := tar.NewReader(xr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		name := filepath.Join(dir, h.Name)
		if err := os.Mkdir(filepath.Dir(name), 0755); err != nil {
			if !os.IsExist(err) {
				return err
			}
		}

		switch h.Typeflag {
		case tar.TypeReg:
			f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.FileMode(h.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.RemoveAll(name); err != nil {
				return err
			}
			if err := os.Symlink(h.Linkname, name); err != nil {
				return err
			}
		}
	}
	return nil
}

func getOS() string {
	goos := runtime.GOOS
	_, err := os.Stat("/etc/alpine-release")
	if os.IsExist(err) {
		return goos + "-alpine"
	}
	return goos
}

// TODO(bmizerany): Add support for 32bit machines?
var archLookup = map[string]string{
	"amd":   "amd64",
	"arm64": "arm64v8",
	"ppc64": "ppc64le",
}

func getArch() string {
	goarch := runtime.GOARCH
	if arch := archLookup[goarch]; arch != "" {
		return arch
	}
	return runtime.GOARCH
}

func slurp(r io.Reader) (*bytes.Reader, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(data), int64(len(data)), nil
}
