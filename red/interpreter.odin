package red

import "core:fmt"
import "core:strconv"
import "core:strings"
import "core:time"

red_command_parse :: proc(command: string) -> [dynamic]string {
	// TODO split string so that command parts can contain spaces by taking into account "some part"
	trimmed := strings.trim_space(command)
	splits := strings.split(trimmed, " ")
	defer delete(splits)
	cleaned_splits := make([dynamic]string, 0, len(splits))
	for split in splits {
		if len(split) > 0 {
			append(&cleaned_splits, split)
		}
	}

	return cleaned_splits
}

red_run_elems :: proc(inst: ^RedinInst, command_parts: [dynamic]string) -> RedinResult {
	command := strings.to_lower(command_parts[0])
	defer delete(command)
	if strings.compare("set", command) == 0 {
		return red_set(inst, command_parts[1], command_parts[2])
	}
	if strings.compare("get", command) == 0 {
		return red_get(inst, command_parts[1])
	}
	if strings.compare("getex", command) == 0 {
		milliseconds, success := strconv.parse_i64(command_parts[2])
		now := time.now()
		ttl := time.time_add(now, time.Duration(milliseconds * 1000))
		if success {
			return red_getex(inst, command_parts[1], ttl)
		}
		return RedinErrorResult{message = "impossible to parts the time to live"}
	}
	if strings.compare("append", command) == 0 {
		return red_append(inst, strings.clone(command_parts[1]), strings.clone(command_parts[2]))
	}
	if strings.compare(command, "del") == 0 {
		if len(command_parts) != 2 do return "false"
		return red_del(inst, command_parts[1])
	}
	if strings.compare(command, "rpush") == 0 {
		count, ok := strconv.parse_uint(command_parts[2])
		if ok do return red_lpop(inst, command_parts[1], count)
		return(
			RedinErrorResult {
				message = "impossible to parse last argument, should be a positive integer",
			} \
		)
	}
	if strings.compare(command, "hmset") == 0 {
		if len(command_parts) < 4 || len(command_parts) %% 2 != 0 do return "0"
		key_vals := make([]string, len(command_parts) - 1)
		defer delete(key_vals)
		for item, index in command_parts[1:] {
			key_vals[index] = strings.clone(item)
		}
		return red_hmset(inst, key_vals[0], ..key_vals[1:])
	}
	if strings.compare(command, "hget") == 0 {
		if len(command_parts) != 3 do return "KO"
		return red_hget(inst, command_parts[1], command_parts[2])
	}
	if strings.compare(command, "hmget") == 0 {
		return red_hmget(inst, command_parts[1], ..command_parts[2:])
	}
	if strings.compare("hkeys", command) == 0 {
		return red_hkeys(inst, command_parts[1])
	}
	if strings.compare("hvals", command) == 0 {
		return red_hvals(inst, command_parts[1])
	}
	if strings.compare("sadd", command) == 0 {
		key_vals := make([]string, len(command_parts) - 1)
		defer delete(key_vals)
		for item, index in command_parts[1:] {
			key_vals[index] = strings.clone(item)
		}
		return red_sadd(inst, key_vals[0], ..key_vals[1:])
	}
	if strings.compare("smembers", command) == 0 {
		return red_smembers(inst, command_parts[1])
	}
	if strings.compare("sismember", command) == 0 {
		return red_sismember(inst, command_parts[1], command_parts[2])
	}
	if strings.compare("keys", command) == 0 {
		return red_keys(inst)
	}
	// TODO add other commands
	return "false"
}

SimpleString :: struct {
	s: string,
}
Null :: struct {}

RedinSimpleResult :: union {
	string,
	SimpleString,
	Maybe(string),
	i64,
	f64,
	bool,
	RedinErrorResult,
}

RedinResult :: union {
	string,
	SimpleString,
	Maybe(string),
	i64,
	f64,
	bool,
	Null,
	[]RedinSimpleResult,
	map[string]RedinSimpleResult,
	map[string]struct {}, //set
	RedinErrorResult,
}

RedinErrorResult :: struct {
	message: string,
}

protocol_line_end :: "\r\n"
nil_string :: "$-1\r\n"

write_simple_string :: proc(builder: ^strings.Builder, r: SimpleString) -> ^strings.Builder {
	strings.write_byte(builder, '+')
	strings.write_string(builder, r.s)
	strings.write_string(builder, protocol_line_end)
	return builder
}

write_i64 :: proc(builder: ^strings.Builder, res: i64) -> ^strings.Builder {
	strings.write_byte(builder, ':')
	strings.write_i64(builder, res)
	strings.write_string(builder, protocol_line_end)
	return builder
}

write_maybe_string :: proc(builder: ^strings.Builder, r: Maybe(string)) -> ^strings.Builder {
	if r == nil {
		strings.write_string(builder, nil_string)
	} else {
		strings.write_byte(builder, '$')
		strings.write_int(builder, len(r.(string)))
		strings.write_string(builder, protocol_line_end)
		strings.write_string(builder, r.(string))
		strings.write_string(builder, protocol_line_end)
	}
	return builder
}

write_f64 :: proc(builder: ^strings.Builder, r: f64) -> ^strings.Builder {
	strings.write_byte(builder, ',')
	strings.write_f64(builder, r, 'f')
	strings.write_string(builder, protocol_line_end)
	return builder
}

write_bool :: proc(builder: ^strings.Builder, r: bool) -> ^strings.Builder {
	strings.write_byte(builder, '#')
	strings.write_byte(builder, r ? 't' : 'f')
	strings.write_string(builder, protocol_line_end)
	return builder
}

result_to_result_string :: proc(res: RedinResult) -> string {
	builder := strings.builder_make()
	switch r in res {
	case RedinErrorResult:
	// TODO proper error response
	case Null:
		strings.write_byte(&builder, '_')
		strings.write_string(&builder, protocol_line_end)
	case string:
		write_maybe_string(&builder, r)
	case SimpleString:
		write_simple_string(&builder, r)
	case Maybe(string):
		write_maybe_string(&builder, r)
	case i64:
		write_i64(&builder, r)
	case f64:
		write_f64(&builder, r)
	case bool:
		write_bool(&builder, r)
	case []RedinSimpleResult:
		strings.write_byte(&builder, '*')
		strings.write_int(&builder, len(r))
		strings.write_string(&builder, protocol_line_end)
		for s in r {
			switch t in s {
			case RedinErrorResult:
			case string:
				write_maybe_string(&builder, t)
			case SimpleString:
				write_simple_string(&builder, t)
			case Maybe(string):
				write_maybe_string(&builder, t)
			case i64:
				write_i64(&builder, t)
			case f64:
				write_f64(&builder, t)
			case bool:
				write_bool(&builder, t)
			}
		}
	case map[string]RedinSimpleResult:
		strings.write_byte(&builder, '%')
		strings.write_int(&builder, len(r))
		strings.write_string(&builder, protocol_line_end)
	// TODO 
	case map[string]struct {}:
		strings.write_byte(&builder, '~')
		strings.write_int(&builder, len(r))
		strings.write_string(&builder, protocol_line_end)
	// TODO
	}

	return strings.to_string(builder)
}

red_interpret :: proc(inst: ^RedinInst, command: string) -> RedinResult {
	elems := red_command_parse(command)
	defer delete(elems)
	return red_run_elems(inst, elems)
}
