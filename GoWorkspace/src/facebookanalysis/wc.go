package main

import "os"
import "io/ioutil"
import "fmt"
import "graphbuilder"
import "container/list"
import "regexp"
import "strings"
import "strconv"
import "log"
import "encoding/json"
import "github.com/jordan-heemskerk/trie"

var pattern *regexp.Regexp

type KeyValue struct {
    Key string
    Value string
}

type NameTreePair struct {
    Total   int
    PrefixTree string
}

func Map(value string) {

    fs := strings.Split(value,"\n")

    for _, v := range fs {

        // Slice with: [wholematch, "Name", "DoW", "HH", "MM"]
        extracted := pattern.FindAllStringSubmatch(v, -1)

        // check if we have a match            
        if len(extracted) > 0 {

            // this will be our key
            name := extracted[0][1]

            // Use DoW later
            // DoW := extracted[0][2]

            // Extract hour and minute strings
            HH := extracted[0][3]
            MM := extracted[0][4]

            // We need to do some parsing for hours to prefix a zero if necessary
            // ie. 0900
            iHH, err := strconv.Atoi(HH)
            
            if err != nil {
                log.Fatal("Not able to convert hour string to an integer")
            }

            sHH := fmt.Sprintf("%02d", iHH) 

            // build value
            v := sHH + MM

            fmt.Printf("%s\t%s\n", name, v)

        }
    }
    
}

func Reduce(key string, values *list.List) {

   // Create a new key result trie
    t := trie.NewTrie()
    total := 0

    // Build tree; add each hour and increase total message count
    for e := values.Front(); e != nil; e = e.Next() {
        total += 1
        t.Add(e.Value.(string))
    }


    b64_trie, _ := t.ToBase64String()
    ntp := NameTreePair{Total: total, PrefixTree: b64_trie}

    encoded, err := json.Marshal(ntp)

    if err != nil {
        log.Fatal(err)
    }

    kv := KeyValue{Key: key, Value: string(encoded)}

    kv_encoded, err := json.Marshal(kv)

    if err != nil {
         log.Fatal(err)
    }

    fmt.Printf("%s\n", string(kv_encoded))
}

func MapReduce(operation string) {

    // TODO: buffer this
    bytes, err := ioutil.ReadAll(os.Stdin)

    if err != nil {
        log.Fatal(err)
    }

    input := string(bytes) 

    if (operation == "map") {

        Map(input)

    } 

    if (operation == "reduce") {

        fs := strings.Split(input,"\n")

        vals := list.New()

        current_key := strings.Split(fs[0],"\t")[0]


	// Hadoop will sort the output from map phase
        // by key first, making this possible
        for _, v := range fs {
 
            if len(v) == 0 {
                continue
            }

            split := strings.Split(v, "\t")
            k := split[0]
            v := split[1]

            vals.PushBack(v)

            if (k != current_key) {

                // Call our old reduce function
                Reduce(current_key, vals)    
                current_key = k
                vals = list.New()

            }

        }

    }


}

/**
 * This program can be run in two ways:
 * 1) MapReduce
 *     a) go run wc.go map 
 *     b) go run wc.go reduce
 * 	
 * 2) Graphbuilder can be built using the base input file name via:
 *     go run wc.go graph stack-hist x.txt 3
 *
 */
func main() {
    pattern = regexp.MustCompile("(.*);([\\w]*),[\\w]*,[\\w]*,[\\w]*,(\\d*):(\\d*)")

    switch len(os.Args) {
    case 2:
          MapReduce(os.Args[1]) 
    case 6:
        // Graph building
        if os.Args[2] == "stack-hist" {
            graphbuilder.BuildGraph(os.Args[3], os.Args[4], os.Args[2], os.Args[5])
        }
    default:
        fmt.Printf("%s: see usage comments in file\n", os.Args[0])
    }
}
