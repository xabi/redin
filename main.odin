package main


import c "core:c/libc"
import "core:fmt"
import "core:log"
import "core:math"
import "core:mem"
import "core:net"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sys/unix"
import "core:thread"
import ds "ds"
import red "red"


buf_size :: 2048

client_worker :: proc(inst: ^red.RedinInst, client_socket: net.TCP_Socket) {
	client_loop: for {
		buf: [buf_size]byte
		builder := strings.builder_from_bytes(buf[:])
		defer strings.builder_destroy(&builder)
		for {
			bytes_read, read_err := net.recv(client_socket, buf[:])
			if read_err != nil {
				fmt.println(read_err)
				fmt.println("client closed connection")
				break client_loop
			}
			strings.write_bytes(&builder, buf[0:bytes_read])
			if bytes_read == 0 do break client_loop
			if bytes_read < buf_size do break

		}
		commands := strings.to_string(builder)
		command_splits := strings.split(commands, "\r\n")
		// defer delete(command_splits)
		for command in command_splits {
			if len(command) == 0 do break
			if strings.compare(command, "QUIT") == 0 do os.exit(0)
			result := red.red_interpret(inst, command)
			buf := red.result_to_result_string(result)
			defer delete(buf)
			fmt.println("repsonse:", buf)
			bytes_written, write_err := net.send_tcp(client_socket, transmute([]byte)buf)
			if write_err != nil {
				fmt.println("an error occured while responding to client", write_err)
			}
		}
	}
}

main :: proc() {
	if ODIN_DEBUG {
		fmt.println("in debug mod")
		// context.logger = log.create_console_logger()
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		context.allocator = mem.tracking_allocator(&track)
		defer {
			if len(track.allocation_map) > 0 {
				fmt.eprintf("=== %v allocations not freed: ===\n", len(track.allocation_map))
				for _, entry in track.allocation_map {
					fmt.eprintf("- %v bytes @ %v\n", entry.size, entry.location)
				}
			}
			if len(track.bad_free_array) > 0 {
				fmt.eprintf("=== %v incorrect frees: ===\n", len(track.bad_free_array))
				for entry in track.bad_free_array {
					fmt.eprintf("- %p @ %v\n", entry.memory, entry.location)
				}
			}
			mem.tracking_allocator_destroy(&track)
		}
	}
	skiplist := ds.SkipList{}
	ds.insert_item(&skiplist, "bonjour", 50)
	ds.insert_item(&skiplist, "hello", 100)
	ds.insert_item(&skiplist, "efbjze", 50)
	ds.insert_item(&skiplist, "efbjzz", 50)
	ds.insert_item(&skiplist, "efbj", 75)
	ds.insert_item(&skiplist, "efbje", 5)
	ds.insert_item(&skiplist, "lastfive", 5)
	ds.insert_item(&skiplist, "afirst", 5)
	ds.insert_item(&skiplist, "afirst", 5)
	ds.insert_item(&skiplist, "zlast", 100)
	ds.insert_item(&skiplist, "zlast50", 50)
	ds.insert_item(&skiplist, "a50", 50)
	ds.insert_item(&skiplist, "z100", 100)
	ds.insert_item(&skiplist, "z50", 50)
	ds.insert_item(&skiplist, "b50", 50)
	ds.insert_item(&skiplist, "a75", 75)
	ds.insert_item(&skiplist, "zbiggest", 110)
	ds.insert_item(&skiplist, "z5", 5)
	ds.insert_item(&skiplist, "z5", 5)
	ds.insert_item(&skiplist, "a5", 5)
	ds.insert_item(&skiplist, "a5", 5)
	ds.insert_item(&skiplist, "a5", 5)
	ds.insert_item(&skiplist, "a5", 5)

	// ds.debug_skip_list(skiplist)

	ds.delete_elems_with_value_and_score(&skiplist, "efbizefhbihfzeb", 23235)
	ds.update_score(&skiplist, "a5", 5, 41234124)
	// ds.delete_elems_with_value_and_score(&skiplist, "hello", 100)
	// head_elems := ds.getdel_lowest_scores(&skiplist, 6)
	// defer delete(head_elems)
	// tail_elems := ds.getdel_highest_scores(&skiplist, 6)
	// defer delete(tail_elems)
	// ds.delete_elems_with_score(&skiplist, 50)
	// ds.debug_skip_list(skiplist)

	score_set := ds.score_set_init()
	ds.insert_or_update(score_set, "bonjour", 100)
	ds.insert_or_update(score_set, "bonjour", 10)
	ds.insert_or_update(score_set, "cool", 4)
	ds.insert_or_update(score_set, "coucou", 12)
	ds.insert_or_update(score_set, "akey", 100)
	ds.insert_or_update(score_set, "yooo", 10)
	ds.insert_or_update(score_set, "aurevoir", 4)
	ds.insert_or_update(score_set, "ciao", 12)

	ds.debug_skip_list(score_set.score_to_key^)

	// inst := red.make_inst()
	// defer free(inst)
	// defer delete(inst^)

	// endpoint, endpoint_parsed := net.parse_endpoint("0.0.0.0:3000")
	// endpoint.port = 3000

	// listen_socket, listen_error := net.listen_tcp(endpoint)

	// for {
	// 	client_socket, client_endpoint, err := net.accept_tcp(listen_socket)
	// 	if err != nil do break
	// 	t := thread.create_and_start_with_poly_data2(
	// 		inst,
	// 		client_socket,
	// 		client_worker,
	// 		context,
	// 		thread.Thread_Priority.Normal,
	// 		true,
	// 	)
	// }

}
