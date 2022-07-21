package event

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/doc"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/hashicorp/go-multierror"
)

type (
	eventData struct {
		PackageName string
		ModulePath  string
		Events      []event
		Deps        []event
	}

	event struct {
		Name   string
		Fields []field
	}

	field struct {
		Name     string
		TypeName string
		Tag      string
	}
)

var registryTmpl = template.Must(template.New("registry-tmpl").Parse(`
/*
 * Package events CODE GENERATED AUTOMATICALLY WITH "github.com/ln80/storer/storer-cli"
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */
package events

import "github.com/ln80/storer/event"

var Registry = event.NewRegister("")
`))

var eventTmpl = template.Must(template.New("event-tmpl").Parse(`
/*
 * Package {{.PackageName}} CODE GENERATED AUTOMATICALLY WITH "github.com/ln80/storer/storer-cli"
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 */
package {{.PackageName}}

import events "{{.ModulePath}}"

{{range .Deps}}
	// {{.Name}} AUTO GENERATED EVENT DEPS. Check the original one in '{{$.PackageName}}' namespace
	type {{.Name}} struct {
		{{range .Fields}}
			{{.Name}} {{.TypeName}} {{.Tag}}{{end}}
	}
{{end}}

{{range .Events}}
	// {{.Name}} AUTO GENERATED EVENT. Check the original one in '{{$.PackageName}}' namespace
	type {{.Name}} struct {
		{{range .Fields}}
			{{.Name}} {{.TypeName}} {{.Tag}}{{end}}
	}
{{end}}

func init() { {{range .Events}}
	events.Registry.Set(&{{.Name}}{}){{end}}
}
`))

func checkDir(path string, target string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("no '%s' directory is passed", target)
	}
	aPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(aPath)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("'%s' path doesn't point to a directory", target)
	}

	return aPath, nil
}

func check(ctx, src, dest, module string) (string, string, string, string, error) {
	if ctx == "" {
		return "", "", "", "", errors.New("no namespace is passed")
	}
	if module == "" {
		return "", "", "", "", errors.New("no dest module is passed")
	}
	srcPath, err := checkDir(src, "src")
	if err != nil {
		return "", "", "", "", err
	}
	destPath, err := checkDir(dest, "dest")
	if err != nil {
		return "", "", "", "", err
	}

	return ctx, srcPath, destPath, module, nil
}

func parseDir(fset *token.FileSet, src string) (map[string]*ast.Package, error) {
	d, err := parser.ParseDir(fset, src, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse 'src' dir code %w", err)
	}
	return d, nil
}

func inspectEvents(fset *token.FileSet, pkgs map[string]*ast.Package) (map[string]*event, map[string]*event, error) {
	events := map[string]*event{}

	deps := map[string]*event{}
	// fetch potential events Types from docs
	for _, f := range pkgs {
		p := doc.New(f, "./", 0)
		for _, t := range p.Types {
			if strings.Contains(t.Doc, "+storer:event-dep") {
				deps[t.Name] = nil
			} else if strings.Contains(t.Doc, "+storer:event") {
				events[t.Name] = nil
			}

		}
	}

	confirmStruct := func(name string) string {
		_, ok1 := events[name]
		if ok1 {
			return "event"
		}
		_, ok2 := deps[name]
		if ok2 {
			return "dep"
		}

		return ""
	}

	var merr error
	for _, f := range pkgs {
		ast.Inspect(f, func(x ast.Node) bool {
			st, ok := x.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if st.Type == nil {
				return true
			}

			kind := confirmStruct(st.Name.Name)
			if kind == "" {
				return true
			}

			s, ok := st.Type.(*ast.StructType)
			if !ok {
				return true
			}
			fields := []field{}
			for _, f := range s.Fields.List {
				if len(f.Names) == 0 {
					merr = multierror.Append(
						merr,
						fmt.Errorf("'%s' event doesn't support embedded fields", st.Name.Name),
					)
					return false
				}
				// resolve field type
				var typeNameBuf bytes.Buffer
				err := printer.Fprint(&typeNameBuf, fset, f.Type)
				if err != nil {
					return true
				}

				if f.Tag != nil {
					fields = append(fields, field{
						Name:     f.Names[0].Name,
						TypeName: typeNameBuf.String(),
						Tag:      f.Tag.Value,
					})
				} else {
					fields = append(fields, field{
						Name:     f.Names[0].Name,
						TypeName: typeNameBuf.String(),
					})
				}

			}

			switch kind {
			case "event":
				events[st.Name.Name] = &event{
					Name:   st.Name.Name,
					Fields: fields,
				}
			case "dep":
				deps[st.Name.Name] = &event{
					Name:   st.Name.Name,
					Fields: fields,
				}
			}

			return false
		})
	}
	// stop the process if some events have invalid field's type
	if merr != nil {
		return nil, nil, merr
	}
	// clean events collection
	for k, ev := range events {
		if ev == nil {
			delete(events, k)
		}
	}
	for k, d := range deps {
		if d == nil {
			delete(deps, k)
		}
	}

	return events, deps, nil
}

func renderEvents(ctx, module string, events map[string]*event, deps map[string]*event) ([]byte, error) {
	var buf bytes.Buffer
	cfg := eventData{PackageName: ctx, ModulePath: module}
	for _, ev := range events {
		cfg.Events = append(cfg.Events, *ev)
	}
	for _, d := range deps {
		cfg.Deps = append(cfg.Deps, *d)

	}

	eventTmpl.Execute(&buf, cfg)

	pretty, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, err
	}

	return pretty, nil
}

func saveCode(code []byte, ctx, dest string) error {
	_, err := os.Stat(fmt.Sprintf("%s/%s/", dest, ctx))
	if os.IsNotExist(err) {
		errDir := os.MkdirAll(fmt.Sprintf("%s/%s/", dest, ctx), 0755)
		if errDir != nil {
			log.Fatal(err)
		}

	}
	f, err := os.Create(fmt.Sprintf("%s/%s/events.go", dest, ctx))
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.Write(code); err != nil {
		return err
	}

	return nil
}

// setupDest checks and populate the event dest repo.
// It creates a top-level package named 'events' which includes
// an event registry singleton.
func setupDest(module, path string) error {
	fset := token.NewFileSet()

	pkgs, err := parseDir(fset, path)
	if err != nil {
		return fmt.Errorf("failed to parse event dest repo")
	}
	if len(pkgs) > 0 {
		for name := range pkgs {
			if name != "events" {
				return fmt.Errorf("dest event repo must only have 'events' as top-level package")
			}
		}
	}

	// Registry template is pretty simple. Cross fingers no error should occur
	var buf bytes.Buffer
	_ = registryTmpl.Execute(&buf, nil)
	code, _ := format.Source(buf.Bytes())

	f, err := os.Create(fmt.Sprintf("%s/registry.go", path))
	if err != nil {
		return fmt.Errorf("failed to create event registry file")
	}
	defer f.Close()

	if _, err = f.Write(code); err != nil {
		return fmt.Errorf("failed to save generated event registry code")
	}

	cmd := exec.Command("go", "mod", "init", module)
	cmd.Dir = path
	_, _ = cmd.Output()

	cmd = exec.Command("go", "mod", "tidy")
	cmd.Dir = path
	_, err = cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to run 'go mod tidy' in event dest repo")
	}

	return nil
}

// Gen exported events
func Export(ctx, src, dest, module string) (string, error) {
	ctx, src, dest, module, err := check(ctx, src, dest, module)
	if err != nil {
		return "", err
	}

	if err := setupDest(module, dest); err != nil {
		return "", err
	}

	fset := token.NewFileSet()
	pkgs, err := parseDir(fset, src)
	if err != nil {
		return "", err
	}

	events, deps, err := inspectEvents(fset, pkgs)
	if err != nil {
		return "", err
	}

	code, err := renderEvents(ctx, module, events, deps)
	if err != nil {
		return "", err
	}

	if err := saveCode(code, ctx, dest); err != nil {
		return "", nil
	}

	return string(code), nil
}
