package red

import "core:fmt"
import "core:strconv"
import "core:strings"
import "core:time"

is_data_valid :: proc(redin_data: RedinData) -> bool {
	if redin_data.valid_until == nil {
		return true
	}
	duration := time.diff(time.now(), redin_data.valid_until.(time.Time))
	return duration > 0
}

internal_get :: proc(inst: ^RedinInst, key: string) -> (data: RedinData, valid: bool, ok: bool) {
	val, exists := inst[key]
	if exists {
		return val, is_data_valid(val), exists
	}
	return val, false, exists
}

internal_cleanup :: proc(val: RedinValue, loc := #caller_location) {
	fmt.println("performing internal cleanup", loc.procedure)
	a: map[string][dynamic]string
	switch d in val {
	case string:
		delete(d)
	case [dynamic]string:
		for s in d {
			delete(s)
		}
		delete(d)
	case map[string]string:
		for k, v in d {
			delete(k)
			delete(v)
		}
		delete(d)
	case map[string]struct {}:
		for k, _ in d {
			delete(k)
		}
		delete(d)
	}
}

red_set :: proc(inst: ^RedinInst, key: string, val: string) -> bool {
	value, valid, ok := internal_get(inst, key)
	if ok || !valid {
		internal_cleanup(value.data)
	}
	inst[key] = RedinData {
		data = strings.clone(val),
	}

	return true
}

red_keys :: proc(inst: ^RedinInst) -> []RedinSimpleResult {
	result := make([]RedinSimpleResult, len(inst))
	i: uint = 0
	for k, _ in inst {
		result[i] = k
		i += 1
	}
	return result
}


red_append :: proc(inst: ^RedinInst, key: string, val: string) -> i64 {
	existing, valid, ok := internal_get(inst, key)
	if !ok {
		inst[key] = RedinData {
			data = strings.clone(val),
		}
		return i64(len(val))
	}
	if ok && !valid {
		internal_cleanup(existing.data)
		inst[key] = RedinData {
			data = strings.clone(val),
		}
		return i64(len(val))
	}
	data := existing.data.(string)
	defer delete(data)

	result := strings.concatenate({data, val})
	inst[key] = RedinData {
		data        = result,
		valid_until = existing.valid_until,
	}
	return i64(len(result))
}
red_setnx :: proc(inst: ^RedinInst, key: string, val: RedinValue) -> i64 {
	data, valid, ok := internal_get(inst, key)
	if ok && valid {
		return 0
	}
	if ok && !valid {
		internal_cleanup(val)
	}
	inst[key] = RedinData {
		data = strings.clone(val.(string)),
	}
	return 1
}

red_getrange :: proc(
	inst: ^RedinInst,
	key: string,
	start: int = int(0),
	end: int = int(0),
) -> string {
	data, valid, ok := internal_get(inst, key)
	if !ok {
		return ""
	}
	if !valid {
		internal_cleanup(data.data)
		delete_key(inst, key)
		return ""
	}
	return strings.cut(data.data.(string), start, end - start)
}

red_get :: proc(inst: ^RedinInst, key: string) -> RedinResult {
	existing, valid, ok := internal_get(inst, key)

	if !ok {
		return Null{}
	}

	if ok && !valid {
		internal_cleanup(existing.data)
		delete_key(inst, key)
		return Null{}
	}

	return existing.data.(string)
}

red_getdel :: proc(inst: ^RedinInst, key: string) -> RedinValue {
	val, valid, ok := internal_get(inst, key)
	if ok {
		internal_cleanup(val.data)
		delete_key(inst, key)
		return val.data
	}
	return nil
}


red_incr :: proc(inst: ^RedinInst, key: string) -> RedinResult {
	return red_incrby(inst, key)
}

red_decr :: proc(inst: ^RedinInst, key: string) -> RedinResult {
	return red_incrby(inst, key, -1)
}

red_decrby :: proc(inst: ^RedinInst, key: string, by: i64) -> RedinResult {
	return red_incrby(inst, key, -by)
}

red_incrby :: proc(inst: ^RedinInst, key: string, by: i64 = 1) -> RedinResult {
	existing, valid, ok := internal_get(inst, key)
	if !ok || !valid {
		by_string := fmt.aprintf("%i", by)
		inst[key] = RedinData {
			data = by_string,
		}
		return by
	}
	#partial switch e in existing.data {
	case string:
		res, success := strconv.parse_i64(e)

		defer delete(e)
		if success {
			incr_value := res + by
			incr_value_string := fmt.aprintf("%i", incr_value)
			inst[key] = RedinData {
				data        = incr_value_string,
				valid_until = existing.valid_until,
			}
			return incr_value
		}
		return RedinErrorResult{message = "impossible to increment a non integer value"}
	case:
		return RedinErrorResult{message = "wrong data type"}
	}
	return 0
}

red_getex :: proc(inst: ^RedinInst, key: string, valid_until: time.Time) -> Maybe(string) {
	val, valid, ok := internal_get(inst, key)
	if !ok {
		return nil
	}
	if !valid {
		delete_key(inst, key)
		return nil
	}
	// TODO check type 
	inst[key] = RedinData {
		data        = val.data,
		valid_until = valid_until,
	}

	return val.data.(string)
}

red_rpush :: proc(inst: ^RedinInst, key: string, vals: ..string) -> uint {
	data, valid, ok := internal_get(inst, key)
	if ok && !valid {
		internal_cleanup(data.data)
		delete_key(inst, key)
		ok = false
	}
	copies := make([]string, len(vals))
	defer delete(copies)
	for v, i in vals {
		copies[i] = strings.clone(v)
	}
	if !ok {
		inst[key] = RedinData {
			data = make([dynamic]string, 0, 16),
		}
		dyn_arr := inst[key].data.([dynamic]string)
		append(&dyn_arr, ..copies)
		inst[key] = RedinData {
			data        = dyn_arr,
			valid_until = data.valid_until,
		}
	} else {
		#partial switch _ in data.data {
		case [dynamic]string:
		case:
			return 0
		}
		dyn_arr := data.data.([dynamic]string)
		append(&dyn_arr, ..copies)
		inst[key] = RedinData {
			data        = dyn_arr,
			valid_until = data.valid_until,
		}
	}
	return len(vals)
}

red_rpop :: proc(inst: ^RedinInst, key: string, count: uint = 1) -> []string {
	data, valid, ok := internal_get(inst, key)
	if !ok {return nil}
	if !valid {
		internal_cleanup(data.data)
		delete_key(inst, key)
		return nil
	}

	#partial switch _ in data.data {
	case [dynamic]string:
	case:
		return nil
	}

	dyn_array := data.data.([dynamic]string)
	results := make([]string, min(len(dyn_array), int(count)))
	for i: uint = 0; i < count; i += 1 {
		res, success := pop_safe(&dyn_array)
		if !success {
			break
		}
		results[i] = res
	}
	inst[key] = RedinData {
		data        = dyn_array,
		valid_until = data.valid_until,
	}
	return results
}

red_lpop :: proc(inst: ^RedinInst, key: string, count: uint = 1) -> []RedinSimpleResult {
	data, valid, ok := internal_get(inst, key)
	if !ok {return nil}
	if !valid {
		internal_cleanup(data.data)
		delete_key(inst, key)
		return nil
	}

	#partial switch _ in data.data {
	case [dynamic]string:
	case:
		return nil
	}

	dyn_array := data.data.([dynamic]string)
	results := make([]RedinSimpleResult, min(len(dyn_array), int(count)))
	for i: uint = 0; i < count; i += 1 {
		// res, success := pop_safe(&data.data.([dynamic]string))
		res, success := pop_front_safe(&dyn_array)
		if !success {
			break
		}
		results[i] = res
	}

	inst[key] = RedinData {
		data        = dyn_array,
		valid_until = data.valid_until,
	}

	return results
}

red_hmset :: proc(inst: ^RedinInst, key: string, key_vals: ..string) -> i64 {
	arg_len := len(key_vals)
	if arg_len % 2 != 0 || arg_len == 0 {
		return 0
	}
	value, valid, ok := internal_get(inst, key)
	if !ok {
		value = RedinData {
			data = make(map[string]string),
		}
		inst[key] = value
	}
	if ok && !valid {
		internal_cleanup(value.data)

		value = RedinData {
			data = make(map[string]string),
		}
		inst[key] = value
	}


	#partial switch _ in value.data {
	case map[string]string:
	case:
		return 0
	}

	i := 0
	data := inst[key].data.(map[string]string)
	for i < arg_len {
		k := key_vals[i]
		v := key_vals[i + 1]
		old_v, hok := data[k]
		if hok {
			delete(old_v)
		}
		data[k] = strings.clone(v)
		i += 2
	}
	inst[key] = RedinData {
		data        = data,
		valid_until = value.valid_until,
	}

	set_count := arg_len / 2
	return i64(set_count)
}

red_hget :: proc(inst: ^RedinInst, key: string, hkey: string) -> Maybe(string) {
	result := red_hmget(inst, key, hkey)
	if result == nil {
		return nil
	}
	return result[0].(Maybe(string))
}

red_hmget :: proc(inst: ^RedinInst, key: string, hkeys: ..string) -> []RedinSimpleResult {
	value, valid, ok := internal_get(inst, key)
	if !ok {
		return nil
	}
	if ok && !valid {
		internal_cleanup(value.data)
		delete_key(inst, key)
		return nil
	}

	#partial switch _ in value.data {
	case map[string]string:
	case:
		return nil
	}

	data := value.data.(map[string]string)
	result := make([]RedinSimpleResult, len(hkeys))
	for key, index in hkeys {
		val, exists := data[key]
		result[index] = exists ? val : nil
	}
	return result
}

red_hkeys :: proc(inst: ^RedinInst, key: string) -> []RedinSimpleResult {
	value, valid, ok := internal_get(inst, key)
	if !ok {
		return nil
	}
	if !valid {
		internal_cleanup(value.data)
		delete_key(inst, key)
		return nil
	}

	#partial switch _ in value.data {
	case map[string]string:
	case:
		return nil
	}

	data := value.data.(map[string]string)
	results := make([]RedinSimpleResult, len(data))
	i := 0
	for key, val in data {
		results[i] = key
		i += 1
	}
	return results
}

red_hvals :: proc(inst: ^RedinInst, key: string) -> []RedinSimpleResult {
	value, valid, ok := internal_get(inst, key)
	if !ok {
		return nil
	}
	if !valid {
		internal_cleanup(value.data)
		delete_key(inst, key)
		return nil
	}

	#partial switch _ in value.data {
	case map[string]string:
	case:
		return nil
	}

	data := value.data.(map[string]string)
	results := make([]RedinSimpleResult, len(data))
	i := 0
	for key, val in data {
		results[i] = val
		i += 1
	}
	return results
}

red_hdel :: proc(inst: ^RedinInst, key: string, hkeys: ..string) -> uint {
	value, valid, ok := internal_get(inst, key)
	if !ok {
		return 0
	}
	if !valid {
		internal_cleanup(value.data)
		delete_key(inst, key)
		return 0
	}

	#partial switch _ in value.data {
	case map[string]string:
	case:
		return 0
	}

	data := value.data.(map[string]string)
	delete_count: uint
	for k in hkeys {
		v, ok := data[k]
		if ok {
			delete(v)
			delete_key(&data, k)
			delete_count += 1
		}
	}
	inst[key] = RedinData {
		data        = data,
		valid_until = value.valid_until,
	}
	return delete_count
}

red_sadd :: proc(inst: ^RedinInst, key: string, elems: ..string) -> i64 {
	value, valid, ok := internal_get(inst, key)
	if !ok {
		value = RedinData {
			data = make(map[string]struct {}),
		}
	}
	if ok && !valid {
		internal_cleanup(value.data)
		value = RedinData {
			data = make(map[string]struct {}),
		}
	}

	#partial switch _ in value.data {
	case map[string]struct {}:
	case:
		return 0
	}

	data := value.data.(map[string]struct {})

	count: i64
	for elem in elems {
		_, is_member := data[elem]
		if !is_member {
			data[elem] = {}
			count += 1
		}
	}

	inst[key] = RedinData {
		data        = data,
		valid_until = value.valid_until,
	}

	return count
}

red_smembers :: proc(inst: ^RedinInst, key: string) -> []RedinSimpleResult {
	value, valid, ok := internal_get(inst, key)
	if !ok {
		return nil
	}
	if ok && !valid {
		internal_cleanup(value.data)
		return nil
	}

	#partial switch _ in value.data {
	case map[string]struct {}:
	case:
		return nil
	}

	data := value.data.(map[string]struct {})
	result := make([]RedinSimpleResult, len(data))
	i := 0
	for k, _ in data {
		result[i] = k
		i += 1
	}
	return result
}

red_sismember :: proc(inst: ^RedinInst, key: string, member: string) -> i64 {
	value, valid, ok := internal_get(inst, key)
	if !ok {
		return 0
	}
	if ok && !valid {
		internal_cleanup(value.data)
		return 0
	}
	#partial switch _ in value.data {
	case map[string]struct {}:
	case:
		return 0
	}
	data := value.data.(map[string]struct {})
	if member in data {
		return 1
	}
	return 0
}

red_del :: proc(inst: ^RedinInst, keys: ..string) -> i64 {
	count: i64
	for k in keys {
		data, _, ok := internal_get(inst, k)
		if !ok {
			continue
		}
		internal_cleanup(data.data)
		delete_key(inst, k)
		count += 1
	}
	return count
}
