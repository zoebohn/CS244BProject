 package locks

import(
    "strings"
    "strconv"
    "fmt"
)

/* JSON util functions. */

 func lock_array_to_string(lock_arr []Lock) string {
    var string_form []string
    for _, l := range lock_arr {
        string_form = append(string_form, string(l))
    }
    str := strings.Join(string_form, ";")
    return str
}

 func string_to_lock_array(s string) []Lock {
    if s == "" {
        return []Lock{}
    }
    string_arr := strings.Split(s, ";")
    var lock_arr []Lock
    for _, l := range string_arr {
        lock_arr = append(lock_arr, Lock(l))
    }
    return lock_arr
 }

func float_array_to_string(float_arr []float64) string {
    var string_form []string
    for _, f := range float_arr {
        string_form = append(string_form, strconv.FormatFloat(f, 'E', -1, 64))
    }
    str := strings.Join(string_form, ";")
    return str
}

 func string_to_float_array(s string) []float64 {
    if s == "" {
        return []float64{}
    }
    string_arr := strings.Split(s, ";")
    var float_arr []float64
    for _, s := range string_arr {
        f, err := strconv.ParseFloat(s, 64)
        if err != nil {
            fmt.Println("Error in string to float array: ", err)
        }
        float_arr = append(float_arr, f)
    }
    return float_arr
 }
