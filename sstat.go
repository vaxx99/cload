package main

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/vaxx99/zload/cnf"
)

type Record struct {
	Id, Sw, Hi, Na, Nb, Ds, De, Dr, Ot, It, Du string
}

type Redrec struct {
	Id string `json:"id"`
	Sw string `json:"sw"`
	Hi string `json:"hi"`
	Na string `json:"na"`
	Nb string `json:"nb"`
	Ds string `json:"ds"`
	De string `json:"de"`
	Dr string `json:"dr"`
	Ot string `json:"ot"`
	It string `json:"it"`
	Du string `json:"du"`
}

type block []Redrec

type hrec struct {
	Rcn string
	Rdr string
	Rec []Record
}

var cfg *cnf.Config

func opendb(path, name string, mod os.FileMode) (*bolt.DB, error) {
	db, err := bolt.Open(path+name, mod, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db, err
}

func bname(ds, de string) (string, string) {
	db, _ := opendb(cfg.Path, "/bdb/"+cfg.Term+"/stat0.db", 0600)
	defer db.Close()
	var bn []string
	var fn string
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("buck"))
		b.ForEach(func(k, v []byte) error {
			bn = append(bn, string(k))
			return nil
		})
		return nil
	})

	max := len(bn) - 1
	buck := bn[max]
	if len(ds) >= 8 {
		for i, j := range bn {
			if j == ds[0:8] {
				buck = bn[i]
			}
		}
	}
	if len(de) >= 8 {
		for i, j := range bn {
			if j == de[0:8] {
				buck = bn[i]
			}
		}
	}
	db.View(func(tx *bolt.Tx) error {
		bckt := tx.Bucket([]byte("buck"))
		v := bckt.Get([]byte(buck))
		if v != nil {
			fn = string(v)
		} else {
			fn = ""
		}
		return nil
	})
	return buck, fn
}

func show(buck, dbn string, w Redrec) hrec {
	var i int
	var rec Record
	var block []Record
	db, _ := opendb(cfg.Path+"/bdb/"+cfg.Term+"/", dbn, 600)
	defer db.Close()
	t1 := time.Now()
	ses := Redrec{}
	if w != ses {
		db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket([]byte(buck))
			i = 0
			b.ForEach(func(k, v []byte) error {
				if find(string(k), w) == true {
					i++
					rec = kval(string(k))
					rec.Id = strconv.Itoa(i)
					block = append(block, rec)
				}
				return nil
			})
			return nil
		})
	}
	t2 := float64(time.Now().Sub(t1).Seconds())
	t3 := strconv.FormatFloat(t2, 'f', 3, 64)
	tr := hrec{
		Rcn: strconv.Itoa(i),
		Rdr: t3,
		Rec: block}
	return tr
}

func call(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		return
	}
	ses := Redrec{}
	what := Redrec{Sw: r.FormValue("sw"), Hi: r.FormValue("hi"), Na: r.FormValue("na"), Nb: r.FormValue("nb"),
		Ds: r.FormValue("ds"), De: r.FormValue("de"), Dr: r.FormValue("dr"), Ot: r.FormValue("ot"),
		It: r.FormValue("it"), Du: r.FormValue("du")}

	if what != ses {
		rc := 0
		buck, dbn := bname(what.Ds, what.De)
		tr := show(buck, dbn, what)
		rc, _ = strconv.Atoi(tr.Rcn)
		switch {
		case rc < 5000:
			a, b := logt(time.Now())
			c := &Logrec{b, r.RemoteAddr, tr.Rcn, tr.Rdr, what}
			slogs(a, c)
			t := template.New("call")
			t, _ = template.ParseFiles("xmp/call.tmpl")
			t.ExecuteTemplate(w, "call", tr)
		default:
			t := template.New("alrm")
			t, _ = template.ParseFiles("xmp/alrm.tmpl")
			t.ExecuteTemplate(w, "alrm", rc)
		}
	} else {
		http.Redirect(w, r, "/form", 301)
	}
}

func home(w http.ResponseWriter, r *http.Request) {
	t := template.New("home")
	t, _ = template.ParseFiles("xmp/home.tmpl")
	ts := dbsize()
	t.ExecuteTemplate(w, "home", ts)
}

func head(w http.ResponseWriter, r *http.Request) {
	t := template.New("head")
	t, _ = template.ParseFiles("xmp/head.tmpl")
	ts := dbsize()
	t.ExecuteTemplate(w, "head", ts.All)
}

func form(w http.ResponseWriter, r *http.Request) {
	t := template.New("form")
	t, _ = template.ParseFiles("xmp/form.tmpl")
	ts := dbsize()
	t.ExecuteTemplate(w, "form", ts.All)
}

func stat(w http.ResponseWriter, r *http.Request) {
	ts := dbsize()
	tt, _ := json.Marshal(ts)
	w.Header().Set("Content-Type", "application/json")
	w.Write(tt)
}

func logs(w http.ResponseWriter, r *http.Request) {
	ts := rlogs(time.Now().Format("20060102"))
	tt, _ := json.Marshal(ts)
	w.Header().Set("Content-Type", "application/json")
	w.Write(tt)
}

func dates(dt string) string {
	rd := ""
	if len(dt) > 0 {
		rd = dt[6:8] + "." + dt[4:6] + "." + dt[0:4] + " " + dt[8:10] + ":" + dt[10:12] + ":" + dt[12:14]
	}
	return rd
}

func find(sf string, w Redrec) bool {
	var fs string
	var fnd bool
	s := reflect.ValueOf(&w).Elem()
	t := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i).String()
		n := t.Field(i).Name
		if f != "" {
			fs = n + "." + f
			if strings.Index(sf, fs) > 0 {
				sf = sf[strings.Index(sf, fs):len(sf)]
				if n == "Du" {
					dr, _ := strconv.Atoi(sf[3:])
					rd, _ := strconv.Atoi(f)
					if rd == dr {
						fnd = true
					} else {
						fnd = false
						return fnd
					}
				}
				fnd = true
			} else {
				fnd = false
				return fnd
			}
		}
	}
	return fnd
}

func kval(fs string) Record {
	val := Record{Id: fs[0:strings.Index(fs, ".Sw.")],
		Sw: fs[strings.Index(fs, ".Sw.")+4 : strings.Index(fs, ".Hi.")],
		Hi: fs[strings.Index(fs, ".Hi.")+4 : strings.Index(fs, ".Na.")],
		Na: fs[strings.Index(fs, ".Na.")+4 : strings.Index(fs, ".Nb.")],
		Nb: fs[strings.Index(fs, ".Nb.")+4 : strings.Index(fs, ".Ds.")],
		Ds: dates(fs[strings.Index(fs, ".Ds.")+4 : strings.Index(fs, ".De.")]),
		De: dates(fs[strings.Index(fs, ".De.")+4 : strings.Index(fs, ".Dr.")]),
		Dr: fs[strings.Index(fs, ".Dr.")+4 : strings.Index(fs, ".Ot.")],
		Ot: fs[strings.Index(fs, ".Ot.")+4 : strings.Index(fs, ".It.")],
		It: fs[strings.Index(fs, ".It.")+4 : strings.Index(fs, ".Du.")],
		Du: fs[strings.Index(fs, ".Du.")+4:]}
	return val
}

func zstat() {
	http.HandleFunc("/stat", stat)
	http.HandleFunc("/logs", logs)
	http.HandleFunc("/", home)
	http.HandleFunc("/form", form)
	http.HandleFunc("/call", call)
	http.Handle("/css/", http.StripPrefix("/css/", http.FileServer(http.Dir("css"))))
	http.Handle("/xmp/", http.StripPrefix("/xmp/", http.FileServer(http.Dir("xmp"))))
	log.Println("Start zstat...")
	log.Println(cfg)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, nil))
}

func main() {

	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	go zstat()
	for {
		time.Sleep(time.Second * 5)
	}
}

type vect struct {
	Date, I0, I1, Pi, Vi, A0, A1, Pa, Va string
}

type vrec struct {
	All string
	Rec []vect
}

func dbsize() vrec {
	var vct vrec
	var vc vect
	var ia, aa, bb string
	db, _ := opendb(cfg.Path, "/bdb/"+cfg.Term+"/stat0.db", 600)
	defer db.Close()
	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("size"))
		vct.All = string(b.Get([]byte("ALL")))
		b.ForEach(func(k, v []byte) error {
			if len(string(k)) == 8 {
				if string(k)[0:6] == cfg.Term {
					aa = string(v)
					vc.Date = string(k)[6:8] + "." + string(k)[4:6] + "." + string(k)[0:4]
					vc.I0 = string(v)
					vc.I1 = ia
					s0, s1 := pers(vc.I0, vc.I1)
					vc.Pi = s0
					vc.Vi = s1
					vc.A0 = aa
					vc.A1 = bb
					bb = aa
					s0, s1 = pers(vc.A0, vc.A1)
					vc.Pa = s0
					vc.Va = s1
					ia = string(v)
					vct.Rec = append(vct.Rec, vc)
				}
			}
			return nil
		})
		return nil
	})
	return vct
}

func pers(s1, s2 string) (string, string) {
	var sp, sv, pp string
	z0, _ := strconv.Atoi(s1)
	z1, _ := strconv.Atoi(s2)
	if z1 > 0 {
		vz := z0 / z1
		if vz > 0 {
			pp = "+"
			sv = "c1"
		} else {
			pp = ""
			sv = "c0"
		}
		sp = pp + strconv.FormatFloat(float64(z0)/float64(z1)*100-100, 'f', 2, 64) + "%"
	} else {
		sv = "bl"
	}
	return sp, sv
}

type Logrec struct {
	Time  string
	Raddr string
	Count string
	Rdur  string
	What  Redrec
}

func logt(t time.Time) (string, string) {
	a := t.Format("15:04:05")
	b := t.UnixNano() % 1e6 / 1e3
	c := strconv.FormatInt(b, 10)
	d := t.Format("20060102150405") + c
	return d, a
}

func slogs(key string, recs *Logrec) {
	db, _ := opendb(cfg.Path, "/bdb/"+cfg.Term+"/stat0.db", 600)
	defer db.Close()
	err := db.Update(func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists([]byte("logs"))
		if err != nil {
			return err
		}
		val, _ := json.Marshal(recs)
		err = bucket.Put([]byte(key), val)

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func rlogs(s string) []Logrec {
	var lg Logrec
	var lgs []Logrec
	db, _ := opendb(cfg.Path, "/bdb/"+cfg.Term+"/stat0.db", 600)
	defer db.Close()

	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("logs"))

		b.ForEach(func(k, v []byte) error {
			if strings.Contains(string(k), s) == true {
				e := json.Unmarshal(v, &lg)
				if e != nil {
					log.Printf("Json logs error: %s", e)
				}
				lgs = append(lgs, lg)
			}
			return nil
		})
		return nil
	})
	return lgs
}
