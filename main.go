// Copyright 2017 Tamás Gulácsi
//
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	_ "gopkg.in/goracle.v2"
)

var replOwner = strings.NewReplacer("'", "''", "\n", " ")

func main() {
	flagConnect := flag.String("connect", os.Getenv("BRUNO_ID"), "database to connect to")
	flagJSON := flag.String("json", "", "json file to read/write from")
	flagDotEngine := flag.String("dot.K", "osage", "dot engine (-K)")
	flagDotFormat := flag.String("dot.T", "svg", "dot output format (-T)")
	flagOut := flag.String("o", "", "output name")
	flag.Parse()

	var db *sql.DB
	if *flagConnect != "" || *flagJSON == "" {
		var err error
		db, err = sql.Open("goracle", *flagConnect)
		if err != nil {
			log.Fatal(errors.Wrap(err, *flagConnect))
		}
		defer db.Close()
	}
	var buf bytes.Buffer

	var ownerW string
	owners := flag.Args()
	if len(owners) != 0 {
		buf.WriteString("AND A.owner IN (")
		for i, owner := range owners {
			if i != 0 {
				buf.WriteByte(',')
			}
			buf.WriteByte('\'')
			buf.WriteString(strings.ToUpper(replOwner.Replace(owner)))
			buf.WriteByte('\'')
		}
		buf.WriteString(")")
		ownerW = buf.String()
	}

	tables, err := getTables(db, *flagJSON, ownerW)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	if *flagOut == "" || *flagOut == "-" {
		defer os.Stdout.Close()
		if err = PrintDOT(os.Stdout, tables); err != nil {
			log.Fatal(err)
		}
		return
	}

	fn := *flagOut
	if ext := filepath.Ext(fn); ext != "" {
		fn = fn[:len(fn)-len(ext)]
	}

	var grp errgroup.Group
	for ext, f := range map[string]func(io.Writer, []Table) error{
		"dot":     PrintDOT,
		"gml":     PrintGML,
		"graphml": PrintGraphML,
	} {
		ext, f := ext, f
		grp.Go(func() error {
			fh, err := os.Create(fn + "." + ext)
			if err != nil {
				return err
			}
			log.Printf("%q: %q", ext, fh.Name())
			err = f(fh, tables)
			if closeErr := fh.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			return err
		})
	}

	if err := grp.Wait(); err != nil {
		log.Fatal(err)
	}

	fh, err := os.Create(strings.TrimSuffix(*flagOut, filepath.Ext(*flagOut)) + "." + *flagDotFormat)
	if err != nil {
		log.Fatal(err)
	}
	defer fh.Close()
	cmd := exec.Command("dot", "-T"+*flagDotFormat, "-K"+*flagDotEngine, *flagOut)
	cmd.Stderr = os.Stderr
	cmd.Stdout = fh
	log.Println(cmd.Args)
	if err := cmd.Run(); err != nil {
		log.Fatal(errors.Wrapf(err, "%v", cmd.Args))
	}
	if err := fh.Close(); err != nil {
		log.Fatal(err)
	}
}

const Dot = "__"

var replDot = strings.NewReplacer(".", Dot, "$", "_")

func name(s ...string) string { return replDot.Replace(strings.Join(s, Dot)) }

func getTables(db *sql.DB, jsonFile string, ownerW string) ([]Table, error) {
	var constraints map[string][]Constraint
	var tables []Table
	if db != nil {
		var grp errgroup.Group
		grp.Go(func() error {
			var err error
			constraints, err = readConstraints(db, ownerW)
			return err
		})

		grp.Go(func() error {
			var err error
			tables, err = readTables(db, ownerW)
			return err
		})
		if err := grp.Wait(); err != nil {
			return tables, err
		}
		if jsonFile != "" {
			fh, err := os.Create(jsonFile)
			if err != nil {
				return tables, err
			}
			defer fh.Close()
			enc := json.NewEncoder(fh)
			if err := enc.Encode(constraints); err != nil {
				return tables, err
			}
			if err := enc.Encode(tables); err != nil {
				return tables, err
			}
			if err := fh.Close(); err != nil {
				return tables, err
			}
		}
	} else {
		fh, err := os.Open(jsonFile)
		if err != nil {
			return tables, err
		}
		defer fh.Close()
		dec := json.NewDecoder(fh)
		if err := dec.Decode(&constraints); err != nil {
			return tables, err
		}
		if err := dec.Decode(&tables); err != nil {
			return tables, err
		}
	}

	for i, t := range tables {
		uCols := make(map[string]struct{}, len(t.Columns))
		for _, c := range constraints[t.Owner+"."+t.Name] {
			if c.Type == "R" {
				t.Constraints = append(t.Constraints, c.TableConstraint)
			} else if c.Type == "P" || c.Type == "U" {
				for _, nm := range c.Columns {
					uCols[nm] = struct{}{}
				}
			}
		}
		for i, c := range t.Columns {
			_, ok := uCols[c.Name]
			t.Columns[i].Unique = ok
		}
		tables[i] = t
	}

	sort.Sort(sort.Reverse(byRankSize(tables)))
	sort.Stable(byNameGroup(tables))

	return tables, nil
}
func PrintDOT(w io.Writer, tables []Table) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	var prevGrp string
	bw.WriteString("digraph {\n")
	for _, t := range tables {
		actGrp := grp(t.Name)
		if prevGrp == "" {
			fmt.Fprintf(bw, "subgraph cluster_%s {\n", actGrp)
			prevGrp = actGrp
		} else if actGrp != prevGrp {
			fmt.Fprintf(bw, "}\nsubgraph cluster_%s {\n", actGrp)
			prevGrp = actGrp
		}
		t.PrintNodeDOT(bw)

	}
	if prevGrp != "" {
		bw.WriteString("}\n")
	}

	for _, t := range tables {
		for _, c := range t.Constraints {
			var col string
			if len(c.Columns) != 0 {
				col = c.Columns[0]
			}
			if c.RemoteTable == "" {
				continue
			}
			fmt.Fprintf(bw, "  %s:%s -> %s:%s [label=%q];\n",
				name(t.Owner, t.Name), col,
				name(c.RemoteOwner, c.RemoteTable), col,
				c.RemoteName)
		}
	}
	bw.WriteString("\n}\n")
	return bw.Flush()
}

func (t Table) PrintNodeDOT(w io.Writer) {
	fmt.Fprintf(w, "  %s [pencolor=white shape=box label=<<TABLE ALIGN=\"LEFT\"><TR><TD ALIGN=\"CENTER\" COLSPAN=\"3\"><B>%s.%s</B></TD></TR> <TR><TD COLSPAN=\"3\">%s</TD></TR>\n",
		name(t.Owner, t.Name), t.Owner, t.Name,
		strings.Replace(html.EscapeString(wordwrap.WrapString(t.Comment, 40)), "\n", "<BR/>\n", -1),
	)
	for _, col := range t.Columns {
		var attrs string
		if col.Unique {
			attrs = " BGCOLOR=\"YELLOW\" "
		}
		fmt.Fprintf(w, "<TR><TD PORT=%q ALIGN=\"LEFT\"%s>%s</TD><TD ALIGN=\"LEFT\">%s</TD><TD ALIGN=\"RIGHT\">%s</TD></TR>\n",
			col.Name,
			attrs, col.Name, html.EscapeString(col.Type),
			strings.Replace(html.EscapeString(wordwrap.WrapString(col.Comment, 25)), "\n", "<BR/>\n", -1),
		)
	}
	io.WriteString(w, "</TABLE>>];\n")
}

func PrintGML(w io.Writer, tables []Table) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	bw.WriteString("graph [\n")
	for _, t := range tables {
		t.PrintNodeGML(bw)
	}

	for _, t := range tables {
		for _, c := range t.Constraints {
			var col string
			if len(c.Columns) != 0 {
				col = c.Columns[0]
			}
			if c.RemoteTable == "" {
				continue
			}
			fmt.Fprintf(bw, "\tedge [\n\t\tsource %s:%s\n\t\ttarget %s:%s\n\t\tlabel %q\n\t]\n",
				name(t.Owner, t.Name), col,
				name(c.RemoteOwner, c.RemoteTable), col,
				c.RemoteName)
		}
	}
	bw.WriteString("]\n")
	return bw.Flush()
}

func (t Table) PrintNodeGML(w io.Writer) {
	fmt.Fprintf(w, "\tnode [\n\t\tid %s\n\t\t%s\n\t]\n",
		name(t.Owner, t.Name),
		html.EscapeString(wordwrap.WrapString(t.Comment, 40)),
	)
}

func PrintGraphML(w io.Writer, tables []Table) error {
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	var prevGrp string
	bw.WriteString("digraph {\n")
	for _, t := range tables {
		actGrp := grp(t.Name)
		if prevGrp == "" {
			fmt.Fprintf(bw, "subgraph cluster_%s {\n", actGrp)
			prevGrp = actGrp
		} else if actGrp != prevGrp {
			fmt.Fprintf(bw, "}\nsubgraph cluster_%s {\n", actGrp)
			prevGrp = actGrp
		}
		t.PrintNodeDOT(bw)

	}
	if prevGrp != "" {
		bw.WriteString("}\n")
	}

	for _, t := range tables {
		for _, c := range t.Constraints {
			var col string
			if len(c.Columns) != 0 {
				col = c.Columns[0]
			}
			if c.RemoteTable == "" {
				continue
			}
			fmt.Fprintf(bw, "  %s:%s -> %s:%s [label=%q];\n",
				name(t.Owner, t.Name), col,
				name(c.RemoteOwner, c.RemoteTable), col,
				c.RemoteName)
		}
	}
	bw.WriteString("\n}\n")
	return bw.Flush()
}

func (t Table) PrintNodeGraphML(w io.Writer) {
	fmt.Fprintf(w, "  %s [pencolor=white shape=box label=<<TABLE ALIGN=\"LEFT\"><TR><TD ALIGN=\"CENTER\" COLSPAN=\"3\"><B>%s.%s</B></TD></TR> <TR><TD COLSPAN=\"3\">%s</TD></TR>\n",
		name(t.Owner, t.Name), t.Owner, t.Name,
		strings.Replace(html.EscapeString(wordwrap.WrapString(t.Comment, 40)), "\n", "<BR/>\n", -1),
	)
	for _, col := range t.Columns {
		var attrs string
		if col.Unique {
			attrs = " BGCOLOR=\"YELLOW\" "
		}
		fmt.Fprintf(w, "<TR><TD PORT=%q ALIGN=\"LEFT\"%s>%s</TD><TD ALIGN=\"LEFT\">%s</TD><TD ALIGN=\"RIGHT\">%s</TD></TR>\n",
			col.Name,
			attrs, col.Name, html.EscapeString(col.Type),
			strings.Replace(html.EscapeString(wordwrap.WrapString(col.Comment, 25)), "\n", "<BR/>\n", -1),
		)
	}
	io.WriteString(w, "</TABLE>>];\n")
}

func readTables(db *sql.DB, ownerW string) ([]Table, error) {
	qryCols := `SELECT A.owner, A.table_name, A.column_name,
  (CASE A.data_type
     WHEN 'NUMBER' THEN (
		 CASE WHEN NVL(A.data_precision, 0) = 0 THEN A.data_type
		   WHEN NVL(A.data_scale, 0) = 0 THEN A.data_type||'('||A.data_precision||')'
		   ELSE A.data_type||'('||A.data_precision||','||A.data_scale||')' END)
     WHEN 'DATE' THEN A.data_type
     ELSE A.data_type||'('||A.data_length||')'
     END)||(CASE A.nullable WHEN 'N' THEN ' NOT NULL' END) data_type,
     B.comments tab_comment,
     C.comments col_comment
  FROM all_col_comments C, all_tab_comments B, all_tab_cols A
  WHERE C.column_name = A.column_name AND C.owner = A.owner AND C.table_name = A.table_name AND
        B.owner = A.owner AND B.table_name = A.table_name AND INSTR(A.table_name, '$') = 0
        ` + ownerW + `
  ORDER BY A.owner, A.table_name, A.column_id`

	rows, err := db.Query(qryCols)
	if err != nil {
		return nil, errors.Wrap(err, qryCols)
	}
	defer rows.Close()

	var tables []Table
	var tp Table
	for rows.Next() {
		var c Column
		var ta Table
		if err := rows.Scan(&ta.Owner, &ta.Name, &c.Name, &c.Type, &ta.Comment, &c.Comment); err != nil {
			return tables, err
		}
		ta.Comment = strings.TrimSpace(ta.Comment)
		c.Comment = strings.TrimSpace(c.Comment)
		ta.Columns = []Column{c}
		if tp.Name == "" {
			tp = ta
			continue
		}
		if !(tp.Owner == ta.Owner && tp.Name == ta.Name) {
			tables = append(tables, tp)
			tp = ta
			continue
		}
		tp.Columns = append(tp.Columns, c)
	}
	if tp.Name != "" {
		tables = append(tables, tp)
	}
	return tables, nil
}

type Table struct {
	Owner, Name, Comment string
	Columns              []Column
	Constraints          []TableConstraint
}
type Column struct {
	Name, Type, Comment string
	Unique              bool
}

type TableConstraint struct {
	Columns                              []string
	RemoteOwner, RemoteTable, RemoteName string
}

type Constraint struct {
	Owner, Name, Type, Table string
	TableConstraint
}

func readConstraints(db *sql.DB, ownerW string) (map[string][]Constraint, error) {
	qryCons := `
SELECT a.owner, a.constraint_name, 'R' constraint_type,
       a.table_name, a.column_name,
       -- referenced pk
       c.r_owner, c_pk.table_name r_table_name, c_pk.CONSTRAINT_NAME r_pk
  FROM all_cons_columns a
  JOIN all_constraints c ON a.owner = c.owner
                        AND a.constraint_name = c.constraint_name
  JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                           AND c.r_constraint_name = c_pk.constraint_name
 WHERE c.constraint_type = 'R' ` + ownerW + `
UNION ALL
SELECT A.owner, A.constraint_name, a.constraint_type,
       B.table_name, B.column_name, NULL r_owner, NULL r_table_name, NULL r_pk
  FROM all_cons_columns B, all_constraints A
  WHERE A.constraint_type IN ('P', 'U') AND
        B.owner = a.owner AND B.constraint_name = A.constraint_name
        ` + ownerW + `
  ORDER BY 1, 2, 3, 4, 5
`

	rows, err := db.Query(qryCons)
	if err != nil {
		return nil, errors.Wrap(err, qryCons)
	}
	defer rows.Close()
	constraints := make(map[string][]Constraint)
	var cp Constraint
	for rows.Next() {
		var colName string
		var ca Constraint
		if err := rows.Scan(
			&ca.Owner, &ca.Name, &ca.Type, &ca.Table, &colName,
			&ca.RemoteOwner, &ca.RemoteTable, &ca.RemoteName,
		); err != nil {
			return constraints, err
		}
		ca.Columns = []string{colName}
		if cp.Name == "" {
			cp = ca
			continue
		}
		if !(ca.Owner == cp.Owner && ca.Name == cp.Name && ca.Type == cp.Type && ca.Table == cp.Table &&
			ca.RemoteOwner == cp.RemoteOwner && ca.RemoteTable == cp.RemoteTable && ca.RemoteName == cp.RemoteName) {
			k := cp.Owner + "." + cp.Table
			constraints[k] = append(constraints[k], cp)
			cp = ca
			continue
		}
		cp.Columns = append(cp.Columns, colName)
	}
	if cp.Name != "" {
		k := cp.Owner + "." + cp.Table
		constraints[k] = append(constraints[k], cp)
	}
	return constraints, nil
}

type byRankSize []Table

func (x byRankSize) Len() int      { return len(x) }
func (x byRankSize) Swap(i, j int) { x[i], x[j] = x[j], x[i] }
func (x byRankSize) Less(i, j int) bool {
	if len(x[i].Constraints) < len(x[j].Constraints) {
		return true
	}
	if len(x[i].Constraints) > len(x[j].Constraints) {
		return false
	}
	return len(x[i].Columns) < len(x[j].Columns)
}

type byNameGroup []Table

func (x byNameGroup) Len() int      { return len(x) }
func (x byNameGroup) Swap(i, j int) { x[i], x[j] = x[j], x[i] }
func (x byNameGroup) Less(i, j int) bool {
	if x[i].Owner < x[j].Owner {
		return true
	}
	if x[i].Owner > x[j].Owner {
		return true
	}
	if grp(x[i].Name) < grp(x[j].Name) {
		return true
	}
	if grp(x[i].Name) > grp(x[j].Name) {
		return false
	}
	return x[i].Name < x[j].Name
}

func grp(s string) string {
	for i, part := range strings.SplitN(s, "_", 3) {
		if i >= 1 && len(part) > 2 {
			return part
		}
	}
	return s
}
