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

func int_array_to_string(int_arr []int) string {
    var string_form []string
    for _, i := range int_arr {
        string_form = append(string_form, strconv.Itoa(i))
    }
    str := strings.Join(string_form, ";")
    return str
}

 func string_to_int_array(s string) []int {
    if s == "" {
        return []int{}
    }
    string_arr := strings.Split(s, ";")
    var int_arr []int
    for _, s := range string_arr {
        i, err := strconv.Atoi(s)
        if err != nil {
            fmt.Println("Error in string to int array: ", err)
        }
        int_arr = append(int_arr, i)
    }
    return int_arr
 }
