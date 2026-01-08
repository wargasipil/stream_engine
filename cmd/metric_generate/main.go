package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/tools/go/packages"
)

func main() {
	// log.Println()
	// fname := "./stream_schema/schema.go"
	var fname string
	if os.Getenv("GOFILE") != "" {
		fname = os.Getenv("GOFILE")
	} else {
		if len(os.Args) == 1 {
			fname = "./example/schema.go"
		} else {
			fname = os.Args[1]
		}

	}

	fset := token.NewFileSet()

	file, err := parser.ParseFile(
		fset,
		fname,
		nil,
		parser.ParseComments,
	)
	if err != nil {
		log.Fatal(err)
	}

	mfile := strings.ReplaceAll(fname, ".go", "_metric.go")
	wfile, err := os.OpenFile(
		mfile,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		0644,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer wfile.Close()

	// getting package path
	packagePath, err := packagePathFromFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(packagePath)

	// writing package name
	wfile.Write([]byte(fmt.Sprintf("package %s\n\n", file.Name.Name)))
	// wfile.WriteString("import \"github.com/wargasipil/stream_engine/stream_core\"\n\n")
	wfile.WriteString(`import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wargasipil/stream_engine/stream_core"
)`)
	wfile.WriteString("\n\n")
	wfile.WriteString("// jangan DIEDIT, file ini generate an dari package github.com/wargasipil/stream_engine\n\n")

	ast.Inspect(file, func(n ast.Node) bool {

		ts, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}

		st, ok := ts.Type.(*ast.StructType)

		if !ok {
			return true
		}

		// writing struct metric
		structName := ts.Name.Name
		metricName := "Metric" + ts.Name.Name
		log.Println("generating", metricName)

		// getting index metric
		var indexMap = map[string]bool{}
		indexPaths := []string{}

		var indexFields strings.Builder
		var counterFuncs strings.Builder
		var initparams strings.Builder
		var initstruct strings.Builder
		var initbody strings.Builder
		var initbodyKey strings.Builder
		initbodyKey.WriteString(`
	var err error

	keys := strings.Split(mkey, "/")
	if len(keys) <= 2 {
		return nil, errors.New("key invalid")
	}
	Name := keys[0]
	names := strings.Split(Name, "_")
	indexkeys := keys[1:]
	key := Name + "/" + strings.Join(indexkeys[:len(names)], "/")
	if len(indexkeys) <= 1 {
		return nil, errors.New("index on key invalid")
	}`)
		initbodyKey.WriteString("\n")

		// creating key
		initbody.WriteString("\tkeys := []string{}\n")
		initbody.WriteString("\tnames := []string{}\n")

		initparams.WriteString("store stream_core.KeyStore")
		initstruct.WriteString("\t\tName: Name,\n")
		initstruct.WriteString("\t\tkey: key,\n")

		mapIndex := map[string]bool{}
		var idfield string
		indexC := 0
		for _, field := range st.Fields.List {

			var haveIndex bool
			if field.Tag != nil {
				tag := field.Tag.Value

				if strings.Contains(tag, "metric:\"id\"") {
					idfield = field.Names[0].Name
					continue
				}

				if strings.Contains(tag, "metric:\"index\"") {
					haveIndex = true
					mapIndex[field.Names[0].Name] = true
				}
			}

			tipe := field.Type.(*ast.Ident)
			name := field.Names[0].Name

			if haveIndex { // generating untuk index
				indexPath := strings.ToLower(strings.ReplaceAll(name, "ID", ""))

				indexMap[name] = true
				indexFields.WriteString("\t// Jangan Diubah letterlek\n\t" + name + " " + tipe.Name + "\n")
				if initparams.Len() > 0 {
					initparams.WriteString(", ")
				}
				initparams.WriteString(name + " " + tipe.Name)
				initstruct.WriteString("\t\t" + name + ": " + name + ",\n")

				var value string
				var forKey string
				switch tipe.Name {
				case "uint64", "int64", "float64":
					value = "fmt.Sprintf(\"%d\", " + name + ")"
				case "string":
					value = "fmt.Sprintf(\"%s\", " + name + ")"
				default:
					value = "not_implemented"
				}

				errdec := `
	if err != nil {
		return nil, err
	}`

				switch tipe.Name {
				case "uint64":
					initbodyKey.WriteString(fmt.Sprintf("\tvar %s uint64\n", name))
					initbodyKey.WriteString(fmt.Sprintf("\t%s, err = strconv.ParseUint(indexkeys[%d], 10, 64)\n", name, indexC))
					initbodyKey.WriteString(errdec)
					initbodyKey.WriteString("\n")
				case "int64":
					initbodyKey.WriteString(fmt.Sprintf("\tvar %s int64\n", name))
					initbodyKey.WriteString(fmt.Sprintf("\t%s, err = strconv.ParseInt(indexkeys[%d], 10, 64)\n", name, indexC))
					initbodyKey.WriteString(errdec)
					initbodyKey.WriteString("\n")
				case "float64":
					// strconv.ParseFloat(s, 64)
					initbodyKey.WriteString(fmt.Sprintf("\tvar %s float64\n", name))
					initbodyKey.WriteString(fmt.Sprintf("\t%s, err = strconv.ParseFloat(indexkeys[%d], 64)\n", name, indexC))
					initbodyKey.WriteString(errdec)
					initbodyKey.WriteString("\n")
				case "string":
					forKey = fmt.Sprintf("var %s string = indexkeys[%d]", name, indexC)
					initbodyKey.WriteString("\t" + forKey + "\n")
				}

				initbody.WriteString("\tkeys = append(keys, " + value + ")\n")
				initbody.WriteString("\tnames = append(names, \"" + indexPath + "\")\n")

				indexPaths = append(indexPaths, indexPath)
				indexC += 1

			} else {
				indexPath := CamelToSnake(name)
				addfunc := cases.Title(language.English).String(tipe.Name)
				// add put
				counterFuncs.WriteString("func (m *" + metricName + ") Put" + name + "(value " + tipe.Name + ") " + tipe.Name + " {\n")
				counterFuncs.WriteString("\treturn m.store.Put" + addfunc + "(m.key + \"/" + indexPath + "\", value)\n")
				counterFuncs.WriteString("}\n\n")
				// add inc
				counterFuncs.WriteString("func (m *" + metricName + ") Inc" + name + "(value " + tipe.Name + ") " + tipe.Name + " {\n")
				counterFuncs.WriteString("\treturn m.store.Inc" + addfunc + "(m.key + \"/" + indexPath + "\", value)\n")
				counterFuncs.WriteString("}\n\n")
				// getValue
				counterFuncs.WriteString("func (m *" + metricName + ") Get" + name + "() " + tipe.Name + " {\n")
				counterFuncs.WriteString("\treturn m.store.Get" + addfunc + "(m.key + \"/" + indexPath + "\")\n")
				counterFuncs.WriteString("}\n\n")
			}

		}

		initbody.WriteString("\tkey := fmt.Sprintf(\"%s/%s\", strings.Join(names, \"_\"), strings.Join(keys, \"/\"))\n")
		initbody.WriteString("\tName := strings.Join(names, \"_\")\n")

		if idfield == "" {
			log.Fatalf("there is no tag metric:\"id\" in struct %s", structName)
		}

		var datastructfunc strings.Builder
		var anydatafunc strings.Builder

		// function getting key

		counterFuncs.WriteString("func (m *" + metricName + ") GetKey() string {\n")
		counterFuncs.WriteString("\treturn m.key\n")
		counterFuncs.WriteString("}\n\n")
		// function getting data struct
		datastructfunc.WriteString("func (m *" + metricName + ") Data() *" + structName + " {\n")
		datastructfunc.WriteString("\treturn &" + structName + "{\n")
		anydatafunc.WriteString("func (m *" + metricName + ") Any() any {\n")
		anydatafunc.WriteString("\treturn &" + structName + "{\n")

		// function getting value
		counterFuncs.WriteString("func (m *" + metricName + ") Values() map[string]any {\n")
		counterFuncs.WriteString("\treturn map[string]any{\n")
		for _, field := range st.Fields.List {
			name := field.Names[0].Name
			if name == idfield {
				counterFuncs.WriteString("\t\t\"" + name + "\": stream_core.HashKeyString(m.key),\n")
				datastructfunc.WriteString("\t\t" + name + ": stream_core.HashKeyString(m.key),\n")
				anydatafunc.WriteString("\t\t" + name + ": stream_core.HashKeyString(m.key),\n")
				continue
			}

			if mapIndex[name] {
				counterFuncs.WriteString("\t\t\"" + name + "\": m." + name + ",\n")
				datastructfunc.WriteString("\t\t" + name + ": m." + name + ",\n")
				anydatafunc.WriteString("\t\t" + name + ": m." + name + ",\n")
			} else {
				counterFuncs.WriteString("\t\t\"" + name + "\": m.Get" + name + "(),\n")
				datastructfunc.WriteString("\t\t" + name + ": m.Get" + name + "(),\n")
				anydatafunc.WriteString("\t\t" + name + ": m.Get" + name + "(),\n")
			}
		}

		counterFuncs.WriteString("\t}\n")
		counterFuncs.WriteString("}\n\n")

		anydatafunc.WriteString("\t}\n")
		anydatafunc.WriteString("}\n\n")
		datastructfunc.WriteString("\t}\n")
		datastructfunc.WriteString("}\n\n")

		var structDec strings.Builder
		var initiate strings.Builder

		structDec.WriteString("type " + metricName + " struct {\n")
		structDec.WriteString("\tkey string\n")
		structDec.WriteString("\tName string\n")
		structDec.WriteString("\tstore stream_core.KeyStore\n")

		structDec.WriteString("\n")
		structDec.WriteString(indexFields.String())
		structDec.WriteString("}\n\n")

		// initiate
		initiate.WriteString("func New" + metricName + "(")
		initiate.WriteString(initparams.String())
		initiate.WriteString(") *" + metricName + " {\n")
		initiate.WriteString(initbody.String() + "\n")
		initiate.WriteString("\treturn &Metric" + ts.Name.Name + "{\n")
		initiate.WriteString("\t\tstore: store,\n")
		initiate.WriteString(initstruct.String())
		initiate.WriteString("\t}\n")
		initiate.WriteString("}\n\n")

		// initiate from key
		initiate.WriteString("func New" + metricName + "FromKey(store stream_core.KeyStore, mkey string) (*" + metricName + ", error) {\n")
		initiate.WriteString(initbodyKey.String() + "\n")
		initiate.WriteString("\treturn &Metric" + ts.Name.Name + "{\n")
		initiate.WriteString("\t\tstore: store,\n")
		initiate.WriteString(initstruct.String())
		initiate.WriteString("\t}, err\n")
		initiate.WriteString("}\n\n")

		// is that struct
		var isThatStruct strings.Builder
		isThatStruct.WriteString("func Is" + metricName + "(key string) bool {\n")
		indexPathStr := strings.Join(indexPaths, "_")
		isThatStruct.WriteString("\treturn strings.HasPrefix(key, \"" + indexPathStr + "/\")\n")
		isThatStruct.WriteString("}\n\n")

		// writing struct
		wfile.WriteString(structDec.String())
		// writing initiate new
		wfile.WriteString(initiate.String())
		wfile.WriteString(isThatStruct.String())
		// writing counter
		wfile.WriteString(counterFuncs.String())
		// writing data function
		wfile.WriteString(datastructfunc.String())
		// writing data any
		wfile.WriteString(anydatafunc.String())

		return false
	})
}

func LogJson(v ...any) {
	for _, item := range v {
		data, _ := json.MarshalIndent(item, "", "  ")
		log.Println(string(data))
	}

}

func ToCamel(s string) string {
	var out strings.Builder
	upper := true

	for _, r := range s {
		if r == '_' || r == '-' || r == ' ' {
			upper = true
			continue
		}
		if upper {
			out.WriteRune(unicode.ToUpper(r))
			upper = false
		} else {
			out.WriteRune(r)
		}
	}
	return out.String()
}

func CamelToSnake(s string) string {
	var out []rune

	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
		} else {
			out = append(out, r)
		}
	}
	return string(out)
}

func packagePathFromFile(filename string) (string, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles,
	}

	pkgs, err := packages.Load(cfg, "file="+filename)
	if err != nil {
		return "", err
	}
	if len(pkgs) == 0 {
		return "", fmt.Errorf("no package found")
	}
	return pkgs[0].PkgPath, nil
}
