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
)

func main() {
	// log.Println()
	// fname := "./stream_schema/schema.go"
	var fname string
	if os.Getenv("GOFILE") != "" {
		fname = os.Getenv("GOFILE")
	} else {
		if len(os.Args) == 1 {
			fname = "./stream_schema/schema.go"
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

	// writing package name
	wfile.Write([]byte(fmt.Sprintf("package %s\n\n", file.Name.Name)))
	// wfile.WriteString("import \"github.com/wargasipil/stream_engine/stream_core\"\n\n")
	wfile.WriteString(`import (
	"fmt"
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

		var indexFields strings.Builder
		var counterFuncs strings.Builder
		var initparams strings.Builder
		var initstruct strings.Builder
		var initbody strings.Builder
		// creating key
		initbody.WriteString("\tkeys := []string{}\n")
		initbody.WriteString("\tnames := []string{}\n")

		initparams.WriteString("store stream_core.KeyStore")
		initstruct.WriteString("\t\tName: strings.Join(names, \"_\"),\n")
		initstruct.WriteString("\t\tkey: strings.Join(keys, \"/\"),\n")

		mapIndex := map[string]bool{}
		var idfield string
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

			indexPath := CamelToSnake(strings.ReplaceAll(name, "ID", ""))

			if haveIndex { // generating untuk index
				indexMap[name] = true
				indexFields.WriteString("\t// Jangan Diubah letterlek\n\t" + name + " " + tipe.Name + "\n")
				if initparams.Len() > 0 {
					initparams.WriteString(", ")
				}
				initparams.WriteString(name + " " + tipe.Name)
				initstruct.WriteString("\t\t" + name + ": " + name + ",\n")

				var value string
				switch tipe.Name {
				case "uint64", "int64", "float64":
					value = "fmt.Sprintf(\"" + indexPath + "/%d\", " + name + ")"
				case "string":
					value = "fmt.Sprintf(\"" + indexPath + "/%s\", " + name + ")"
				default:
					value = "not_implemented"
				}

				initbody.WriteString("\tkeys = append(keys, " + value + ")\n")
				initbody.WriteString("\tnames = append(names, \"" + indexPath + "\")\n")

			} else {
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

		if idfield == "" {
			log.Fatalf("there is no tag metric:\"id\" in struct %s", structName)
		}

		var datastructfunc strings.Builder

		counterFuncs.WriteString("func (m *" + metricName + ") GetKey() string {\n")
		counterFuncs.WriteString("\treturn m.key\n")
		counterFuncs.WriteString("}\n\n")
		// function getting data struct
		datastructfunc.WriteString("func (m *" + metricName + ") Data() *" + structName + " {\n")
		datastructfunc.WriteString("\treturn &" + structName + "{\n")

		// function getting value
		counterFuncs.WriteString("func (m *" + metricName + ") Values() map[string]any {\n")
		counterFuncs.WriteString("\treturn map[string]any{\n")
		for _, field := range st.Fields.List {
			name := field.Names[0].Name
			if name == idfield {
				counterFuncs.WriteString("\t\t\"" + name + "\": stream_core.HashKeyString(m.key),\n")
				datastructfunc.WriteString("\t\t" + name + ": stream_core.HashKeyString(m.key),\n")
				continue
			}

			if mapIndex[name] {
				counterFuncs.WriteString("\t\t\"" + name + "\": m." + name + ",\n")
				datastructfunc.WriteString("\t\t" + name + ": m." + name + ",\n")
			} else {
				counterFuncs.WriteString("\t\t\"" + name + "\": m.Get" + name + "(),\n")
				datastructfunc.WriteString("\t\t" + name + ": m.Get" + name + "(),\n")
			}
		}

		counterFuncs.WriteString("\t}\n")
		counterFuncs.WriteString("}\n\n")

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

		// writing struct
		wfile.WriteString(structDec.String())
		// writing initiate new
		wfile.WriteString(initiate.String())
		// writing counter
		wfile.WriteString(counterFuncs.String())
		// writing data function
		wfile.WriteString(datastructfunc.String())

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
