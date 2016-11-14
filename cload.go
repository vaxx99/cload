package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/vaxx99/cload/cnf"
)

type Flags struct {
	f01 int64
	f02 int64
	f03 int64
	f04 int64
	f05 int64
	f06 int64
	f07 int64
	f08 int64
	f09 int64
	f10 int64
	f11 int64
	f12 int64
	f13 int64
	f14 int64
	f15 int64
	f16 int64
	f17 int64
	f18 int64
	f19 int64
}

type Frec struct {
	IDR  int64
	IDC  int64
	FLG  Flags
	SLS  int64
	CHS  int64
	ZCL  int64
	SLL  int64
	ZCD  string
	SNC  string
	P100 P100
	P102 P102
	P103 P103
	P104 P104
	P105 P105
	P106 P106
	P107 P107
	P108 P108
	P109 P109
	P110 P110
	P111 P111
	P112 P112
	P113 P113
	P114 P114
	P115 P115
	P116 P116
	P119 P119
	P121 P121
}

type P100 struct {
	IDI int64
	CNL int64
	CNC string
}

type P102 struct {
	IDI int64
	DTS string
	F1  int64
}

type P103 struct {
	IDI int64
	DTE string
	F1  int64
}

type P104 struct {
	IDI int64
	CNT int64
}

type P105 struct {
	IDI int64
	SVC int64
	TVC int64
}

type P106 struct {
	IDI int64
	SVC int64
}

type P107 struct {
	IDI int64
	SVC int64
}

type P108 struct {
	IDI int64
	TPE int64
	SVC int64
}

type P109 struct {
	IDI int64
	CNL int64
	CNC string
}

type P110 struct {
	IDI int64
	CAT int64
}

type P111 struct {
	IDI int64
	DIR int64
}

type P112 struct {
	IDI int64
	CFC int64
}

type P113 struct {
	IDI int64
	TGN int64
	SLN int64
	MDN int64
	PTN int64
	CHN int64
}

type P114 struct {
	IDI int64
	TGN int64
	SLN int64
	MDN int64
	PTN int64
	CHN int64
}

type P115 struct {
	IDI int64
	DUR int64
}

type P116 struct {
	IDI int64
	BTL int64
	CRC int64
}

type P119 struct {
	IDI int64
	BTL int64
	CNL int64
	CNC string
}

type P121 struct {
	IDI int64
	BTL int64
	COI int64
	CNC string
}

var wd, sp string

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

var cfg *cnf.Config

func opendb(path, name string, mod os.FileMode) *bolt.DB {
	db, err := bolt.Open(path+"/"+name, mod, nil)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func term(c *cnf.Config) {
	os.Mkdir(cfg.Path+"/bdb/"+cfg.Term, 0777)
	if time.Now().Format("20060102")[6:8] == "01" && time.Now().Format("150405")[0:2] == "06" {
		fmt.Println("current period:", time.Now().Format("200601"))
		s := `{"Path":"` + c.Path + `","Port":"` + c.Port + `","Term":"` + time.Now().Format("200601") + `"}`
		d := []byte(s)
		os.Mkdir(cfg.Path+"/bdb/"+time.Now().Format("200601"), 0777)
		err := ioutil.WriteFile("conf.json", d, 0644)
		check(err)
	}
}

func week(day string) string {
	var s string
	switch day {
	case "01", "02", "03", "04", "05", "06", "07":
		s = "week01"
	case "08", "09", "10", "11", "12", "13", "14":
		s = "week02"
	case "15", "16", "17", "18", "19", "20", "21":
		s = "week03"
	case "22", "23", "24", "25", "26", "27", "28", "29", "30", "31":
		s = "week04"
	}
	return s
}

func wize(db *bolt.DB) {
	t := time.Now()
	days := map[string]int{}
	bckn := map[string]string{}
	os.Chdir(cfg.Path + "/bdb/" + cfg.Term)
	f, _ := ioutil.ReadDir(".")
	for _, fn := range f {
		if fn.Name()[0:4] == "week" {
			wb := opendb(cfg.Path+"/bdb/"+cfg.Term+"/", fn.Name(), 0600)
			bn := bname(wb)
			for _, buckn := range bn {
				bckn[buckn] = fn.Name()
				wb.View(func(tx *bolt.Tx) error {
					// Assume bucket exists and has keys
					b := tx.Bucket([]byte(buckn))
					b.ForEach(func(k, v []byte) error {
						days["ALL"]++
						days[string(k)[0:6]]++
						days[string(k)[0:8]]++
						return nil
					})
					return nil
				})
			}
			wb.Close()
		}
	}
	db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("size"))
		for k, v := range days {
			kv := strconv.Itoa(v)
			err = bucket.Put([]byte(k), []byte(kv))
		}
		return err
	})
	db.Update(func(tx *bolt.Tx) error {
		bckt, err := tx.CreateBucketIfNotExists([]byte("buck"))
		for k, v := range bckn {
			err = bckt.Put([]byte(k), []byte(v))
		}
		return err
	})
	t1 := time.Now().Sub(t).Seconds()
	fmt.Printf("%4s %10d %10s %8.3f\n", "size:", days["ALL"], time.Now().Format("15:04:05"), t1)
}

func rset(recs []Redrec, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		for _, v := range recs {
			bucket, err := tx.CreateBucketIfNotExists([]byte(v.Id[0:8]))
			if err != nil {
				return err
			}
			key := v.Id + ".Sw." + v.Sw + ".Hi." + v.Hi + ".Na." + v.Na + ".Nb." + v.Nb + ".Ds." + v.Ds + ".De." + v.De +
				".Dr." + v.Dr + ".Ot." + v.Ot + ".It." + v.It + ".Du." + v.Du

			err = bucket.Put([]byte(key), []byte(v.Id[0:6]))

		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func Dates(dt string) string {
	rd := ""
	if len(dt) > 0 {
		rd = dt[6:8] + "." + dt[4:6] + "." + dt[0:4] + " " + dt[8:10] + ":" + dt[10:12] + ":" + dt[12:14]
	}
	return rd
}

func rget(buck, key string, db *bolt.DB) {
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(buck))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", buck)
		}

		val := bucket.Get([]byte(key))
		fmt.Println(string(val))
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func fget(key string, db *bolt.DB) bool {
	var f bool
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("file"))
		if bucket == nil {
			f = false
			return nil
		}

		val := bucket.Get([]byte(key))
		if val != nil {
			f = true
		} else {
			f = false
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
	return f
}

func set(buck, key, val string, db *bolt.DB) {
	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(buck))
		if err != nil {
			return err
		}

		err = bucket.Put([]byte(key), []byte(val))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func bname(db *bolt.DB) []string {
	var bn []string
	db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if string(k)[0:4] != "file" && string(k)[0:4] != "size" {
				bn = append(bn, string(k))
			}
		}
		return nil
	})
	return bn
}

func s2002rec(sw string, srec Frec) Redrec {
	var rec Redrec
	rec.Id = srec.P102.DTS
	rec.Sw = sw
	rec.Hi = strconv.Itoa(int(srec.P110.CAT))
	rec.Na = srec.SNC
	rec.Nb = srec.P100.CNC
	rec.Ds = srec.P102.DTS
	rec.De = srec.P103.DTE
	rec.Dr = strconv.Itoa(int(srec.P111.DIR))
	rec.It = strconv.Itoa(int(srec.P113.TGN))
	rec.Ot = strconv.Itoa(int(srec.P114.TGN))
	rec.Du = strconv.FormatFloat(float64(srec.P115.DUR)/1000, 'f', 2, 64)
	return rec
}

func main() {

	fmt.Println("si2k loader:", time.Now().Format("02.01.2006 15:04:05"))
	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	os.Chdir(cfg.Path)
	term(cfg)
	cnf.LoadConfig()
	cfg = cnf.GetConfig()
	os.Chdir(cfg.Path + "/tmp")
	st0 := opendb(cfg.Path+"/bdb/"+cfg.Term, "stat0.db", 0666)
	defer st0.Close()
	f, _ := ioutil.ReadDir(".")
	ds := false
	for _, fn := range f {
		if issi(fn.Name()) {
			if fget(fn.Name(), st0) != true {
				var w1, w2, w3, w4 block
				ds = true
				t0 := time.Now()
				cnt, sw, mn, rp := si2k(fn.Name())
				set("file", fn.Name(), mn[0:8], st0)
				for _, v := range rp {
					if v.Id[0:6] == cfg.Term {
						switch week(v.Id[6:8]) {
						case "week01":
							w1 = append(w1, v)
						case "week02":
							w2 = append(w2, v)
						case "week03":
							w3 = append(w3, v)
						case "week04":
							w4 = append(w4, v)
						}
					}
				}
				if len(w1) > 0 {
					wb1 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week1.db", 0666)
					rset(w1, wb1)
					wb1.Close()
				}
				if len(w2) > 0 {
					wb2 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week2.db", 0666)
					rset(w2, wb2)
					wb2.Close()
				}
				if len(w3) > 0 {
					wb3 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week3.db", 0666)
					rset(w3, wb3)
					wb3.Close()
				}
				if len(w4) > 0 {
					wb4 := opendb(cfg.Path+"/bdb/"+cfg.Term, "week4.db", 0666)
					rset(w4, wb4)
					wb4.Close()
				}
				t1 := time.Now().Sub(t0).Seconds()
				log.Printf("%25s %10s %10s %8d %8s %8.2f\n", fn.Name(), sw, mn[0:8], cnt, "load", t1)
			}
			os.Remove(fn.Name())
		}
	}
	if ds {
		wize(st0)
	}
	//fmt.Println("*")
}

func issi(fn string) bool {
	f, _ := os.Open(fn)
	defer f.Close()
	data, _ := Read(f, 1)
	ad := H2c(data)
	switch ad {
	case "C8":
		return true
	case "D2":
		return true
	case "D3":
		return true
	case "D4":
		return true
	}

	return false
}

func Read(file *os.File, bt int) ([]byte, error) {
	data := make([]byte, bt)
	_, e := file.Read(data)
	if e != nil {
		log.Println("File open error:", e)
	}
	return data, e
}

func b2i(b []byte) int64 {
	hd := strings.ToUpper(hex.EncodeToString(b))
	res, _ := strconv.ParseInt(hd, 16, 64)
	return res
}

func bc2i(b string) int64 {
	a, _ := strconv.ParseInt(b, 2, 64)
	return a
}

func H2c(dt []byte) string {
	hd := strings.ToUpper(hex.EncodeToString(dt))
	return hd
}

func Oct(b byte) string {
	return fmt.Sprintf("%08b", b)
}

func Bts(d int64) int {
	return int(float64(d)/2 + 0.5)
}

func flags(s string) Flags {
	var f []int64
	for _, j := range s {
		res, _ := strconv.ParseInt(string(j), 10, 64)
		f = append(f, res)
	}
	return Flags{f[7], f[6], f[5], f[4], f[3], f[2], f[1], f[7], f[15], f[14], f[13], f[12], f[11], f[10], f[9], f[8], f[18], f[17], f[16]}
}

func dates(b []byte) string {
	rd := ""
	if len(b) > 0 {
		rd = "20" + dd(int(b[0])) + dd(int(b[1])) + dd(int(b[2])) + dd(int(b[3])) +
			dd(int(b[4])) + dd(int(b[5])) //+ dd(int(b[6]))
	}
	return rd
}

func dd(d int) string {
	if d < 10 {
		return "0" + strconv.Itoa(d)
	}
	return strconv.Itoa(d)
}

func si2k(fn string) (int64, string, string, block) {
	f, _ := os.Open(fn)
	var cnt int64
	var sw, mtm string
	var rec Frec
	var Rec block
	defer f.Close()

	for {
		head := make([]byte, 3)
		_, e := f.Read(head)

		if e != nil {
			break
		}

		i := b2i(head[1:3])
		switch head[0] {
		case 200:
			b := make([]byte, i-3)
			_, e = f.Read(b)
			rec = s200(b, i-3)
			cnt++
			sw = fn[1:5]
			Rec = append(Rec, s2002rec(sw, rec))
			if len(rec.P102.DTS) > 0 {
				mtm = rec.P102.DTS
			}
		case 210:
			b := make([]byte, 13)
			_, e = f.Read(b)

		case 211:
			b := make([]byte, 13)
			_, e = f.Read(b)

		case 212:
			b := make([]byte, 6)
			_, e = f.Read(b)
		}
	}
	return cnt, sw, mtm, Rec
}

func s200(b []byte, bs int64) Frec {
	var srec Frec
	//Индекс записи
	srec.IDR = b2i(b[0:4])
	//Идентификатор вызова
	srec.IDC = b2i(b[4:8])
	//Flags
	fb := Oct(b[8]) + Oct(b[9]) + Oct(b[10])
	srec.FLG = flags(fb)

	bc := Oct(b[11])
	//Последовательность
	a, _ := strconv.ParseInt(bc[:4], 2, 8)
	//Состояние учета	стоимости
	c, _ := strconv.ParseInt(bc[4:], 2, 8)
	srec.SLS = a
	srec.CHS = c

	bc = Oct(b[12])
	//Длина кода зоны
	d, _ := strconv.ParseInt(bc[:3], 2, 8)
	//Длина списочного номера
	f, _ := strconv.ParseInt(bc[3:], 2, 8)
	srec.ZCL = d
	srec.SLL = f
	//Bytes count
	btz := Bts(d)
	btn := Bts(f)
	//Код зоны
	srec.ZCD = H2c(b[13 : 13+btz])[0:d]
	//Списочный номер абонента
	srec.SNC = H2c(b[13 : 13+btz+btn])[0 : d+f]

	//dynamic part start byte
	nb := 13 + int((float64(d)+float64(f))/2+0.5)
	for nb < int(bs) {
		id, _ := strconv.ParseInt(Oct(b[nb]), 2, 64)
		nb = dynp(id, nb, b, &srec)
		//fmt.Println(id, nb, bs, &srec)
	}

	return srec
}

func dynp(id int64, nb int, b []byte, rec *Frec) int {
	switch id {
	case 100:
		var st P100
		st.IDI = id
		dc, _ := strconv.ParseInt(Oct(b[nb+1]), 2, 64)
		st.CNL = dc
		btb := Bts(dc)
		st.CNC = H2c(b[nb+2 : nb+2+btb])[0:int(st.CNL)]
		nb = nb + 2 + btb
		rec.P100 = st
		return nb
	case 102:
		var st P102
		st.IDI = id
		bc := Oct(b[nb+9])
		f1, _ := strconv.ParseInt(bc[7:], 2, 64)
		st.F1 = f1
		st.DTS = dates(b[nb+1 : nb+8])
		nb = nb + 9
		rec.P102 = st
		return nb
	case 103:
		var st P103
		st.IDI = id
		bc := Oct(b[nb+9])
		f1, _ := strconv.ParseInt(bc[7:], 2, 64)
		st.F1 = f1
		st.DTE = dates(b[nb+1 : nb+8])
		nb = nb + 9
		rec.P103 = st
		return nb
	case 104:
		var st P104
		bc := Oct(b[nb+1]) + Oct(b[nb+2]) + Oct(b[nb+3])
		//Идентификатор информационного элемента (104)
		st.IDI = id
		cnt, _ := strconv.ParseInt(bc, 2, 64)
		//Количество тарифных импульсов
		st.CNT = cnt
		nb = nb + 4
		rec.P104 = st
		return nb
	case 105:
		var st P105
		//Идентификатор информационного элемента (105)
		st.IDI = id
		//Базовая услуга
		st.SVC, _ = strconv.ParseInt(Oct(b[nb+1]), 2, 64)
		//Телеслужбы
		st.TVC, _ = strconv.ParseInt(Oct(b[nb+2]), 2, 64)
		nb = nb + 3
		rec.P105 = st
		return nb
	case 106:
		var st P106
		//Идентификатор информационного элемента (106)
		st.IDI = id
		//Дополнительная услуга
		st.SVC, _ = strconv.ParseInt(Oct(b[nb+1]), 2, 64)
		nb = nb + 2
		rec.P106 = st
		return nb
	case 107:
		var st P107
		st.IDI = id
		st.SVC, _ = strconv.ParseInt(Oct(b[nb+1]), 2, 64)
		nb = nb + 2
		rec.P107 = st
		return nb
	case 108:
		var st P108
		st.IDI = id
		st.TPE = b2i(b[nb+1 : nb+2])
		st.SVC = b2i(b[nb+2 : nb+3])
		nb = nb + 3
		rec.P108 = st
		return nb
	case 109:
		var st P109
		st.IDI = id
		st.CNL = b2i(b[nb+1 : nb+2])
		btb := Bts(st.CNL)
		st.CNC = H2c(b[nb+2 : nb+2+btb])[0:int(st.CNL)]
		nb = nb + 2 + btb
		rec.P109 = st
		return nb
	case 110:
		var st P110
		//Идентификатор информационного элемента (110)
		st.IDI = id
		//Исходящая категория
		st.CAT, _ = strconv.ParseInt(Oct(b[nb+1]), 2, 64)
		nb = nb + 2
		rec.P110 = st
		return nb
	case 111:
		var st P111
		st.IDI = id
		//Тарифное направление
		st.DIR, _ = strconv.ParseInt(Oct(b[nb+1]), 2, 64)
		nb = nb + 2
		rec.P111 = st
		return nb
	case 112:
		var st P112
		st.IDI = id
		st.CFC = b2i(b[nb+1 : nb+3])
		nb = nb + 2
		rec.P112 = st
		return nb
	case 113:
		var st P113
		st.IDI = id
		st.TGN = b2i(b[nb+1 : nb+3])
		st.SLN = b2i(b[nb+3 : nb+5])
		st.MDN = b2i(b[nb+5 : nb+6])
		st.PTN = b2i(b[nb+6 : nb+8])
		st.CHN = b2i(b[nb+8 : nb+9])
		rec.P113 = st
		nb = nb + 9
		return nb
	case 114:
		var st P114
		st.IDI = id
		st.TGN = b2i(b[nb+1 : nb+3])
		st.SLN = b2i(b[nb+3 : nb+5])
		st.MDN = b2i(b[nb+5 : nb+6])
		st.PTN = b2i(b[nb+6 : nb+8])
		st.CHN = b2i(b[nb+8 : nb+9])
		rec.P114 = st
		nb = nb + 9
		return nb
	case 115:
		var st P115
		st.IDI = id
		st.DUR = b2i(b[nb+1 : nb+5])
		rec.P115 = st
		nb = nb + 5
		return nb
	case 116:
		var st P116
		st.IDI = id
		st.BTL = b2i(b[nb+1 : nb+2])
		st.CRC = b2i(b[nb+2 : nb+4])
		rec.P116 = st
		nb = nb + 4
		return nb
	case 119:
		var st P119
		st.IDI = id
		st.BTL = b2i(b[nb+1 : nb+2])
		st.CNL = b2i(b[nb+2 : nb+3])
		btb := Bts(st.CNL)
		st.CNC = H2c(b[nb+3 : nb+3+btb])[0:int(st.CNL)]
		nb = nb + 3 + btb
		rec.P119 = st
		return nb
	case 121:
		var st P121
		st.IDI = id
		st.BTL = b2i(b[nb+1 : nb+2])
		st.COI = b2i(b[nb+2 : nb+4])
		st.CNC = Oct(b[nb+5])
		nb = nb + 5
		rec.P121 = st
		return nb
	}
	fmt.Println("IDDD:", id)
	os.Exit(0)
	return 0
}
