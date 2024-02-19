package ds

import "core:fmt"
import "core:math/bits"
import "core:math/rand"
import "core:mem"
import "core:strings"

println :: fmt.println
default_max_level :: 32

Element :: struct {
	value: string,
	score: f64,
	prev:  [default_max_level]^Element,
	next:  [default_max_level]^Element,
	level: int,
}

ResultElem :: struct {
	value: string,
	score: f64,
}

SkipList :: struct {
	start_levels:       [default_max_level]^Element,
	end_levels:         [default_max_level]^Element,
	instance_max_level: int,
	element_count:      int,
}


u32_center :: bits.U32_MAX / 2
get_random_level :: proc(skip_list: ^SkipList) -> int {
	level: int
	for rand.uint32() < u32_center do level += 1
	if level > skip_list.instance_max_level {
		level = skip_list.instance_max_level + 1
		skip_list.instance_max_level = level
	}
	return level < default_max_level ? level : default_max_level - 1
}

insert_item :: proc(
	skip_list: ^SkipList,
	val: string,
	score: f64,
	allocator := context.allocator,
) {
	element := new(Element, allocator)
	element^ = Element {
		value = val,
		score = score,
	}

	level := get_random_level(skip_list)
	element.level = level

	skip_list.element_count += 1

	current_elem: ^Element
	for l := skip_list.instance_max_level; l >= 0; l -= 1 {
		current_elem = insert_at_level(skip_list, element, l, current_elem)
	}
}

insert_at_level :: proc(
	skip_list: ^SkipList,
	elem: ^Element,
	level: int,
	current_elem: ^Element = nil,
) -> ^Element {
	to_insert := level <= elem.level

	if skip_list.start_levels[level] == nil && to_insert {
		skip_list.start_levels[level] = elem
		skip_list.end_levels[level] = elem
		return nil
	}

	current_elem := current_elem == nil ? skip_list.start_levels[level] : current_elem
	previous_elem: ^Element
	i := 0
	for current_elem != nil &&
	    (current_elem.score < elem.score ||
			    (current_elem.score == elem.score &&
					    strings.compare(elem.value, current_elem.value) >= 0)) {
		previous_elem = current_elem
		current_elem = current_elem.next[level]
		i += 1
	}
	if !to_insert {
		return previous_elem
	}

	if current_elem == nil {
		previous_elem.next[level] = elem
		elem.prev[level] = previous_elem
		skip_list.end_levels[level] = elem
	} else if previous_elem == nil {
		skip_list.start_levels[level] = elem
		elem.next[level] = current_elem
		current_elem.prev[level] = elem
	} else {
		previous_elem.next[level] = elem
		elem.prev[level] = previous_elem
		elem.next[level] = current_elem
		current_elem.prev[level] = elem
	}

	return previous_elem
}

delete_elems_with_value_and_score :: proc(
	skip_list: ^SkipList,
	value: string,
	score: f64,
	allocator := context.allocator,
) -> int {

	current_elem := skip_list.start_levels[skip_list.instance_max_level]
	if current_elem == nil do return 0

	for l := skip_list.instance_max_level; l >= 0; l -= 1 {
		next := current_elem.next[l]
		prev := current_elem.prev[l]
		smaller :=
			current_elem.score < score ||
			(current_elem.score == score && strings.compare(current_elem.value, value) == -1)
		next_smaller_or_equal :=
			next != nil &&
			(next.score < score ||
					(next.score == score && strings.compare(next.value, value) <= 0))
		greater_or_equal :=
			current_elem.score > score ||
			(current_elem.score == score && strings.compare(current_elem.value, value) >= 0)
		prev_greater_or_equal :=
			prev != nil &&
			(prev.score > score ||
					(prev.score == score && strings.compare(prev.value, value) >= 0))
		if smaller && next_smaller_or_equal {
			for smaller && next_smaller_or_equal {
				current_elem = next
				next = current_elem.next[l]
				smaller =
					current_elem.score < score ||
					(current_elem.score == score &&
							strings.compare(current_elem.value, value) == -1)
				next_smaller_or_equal =
					next != nil &&
					(next.score < score ||
							(next.score == score && strings.compare(next.value, value) <= 0))
			}
		} else if greater_or_equal && prev_greater_or_equal {
			for greater_or_equal && prev_greater_or_equal {
				current_elem = prev
				prev = current_elem.prev[l]
				greater_or_equal =
					current_elem.score > score ||
					(current_elem.score == score &&
							strings.compare(current_elem.value, value) >= 0)
				prev_greater_or_equal =
					prev != nil &&
					(prev.score > score ||
							(prev.score == score && strings.compare(prev.value, value) >= 0))
			}
		}
	}
	count := 0
	for current_elem != nil &&
	    current_elem.score == score &&
	    strings.compare(current_elem.value, value) == 0 {
		count += 1
		next_elem := current_elem.next[0]
		delete_current_elem(skip_list, current_elem)
		current_elem = next_elem
	}
	return count
}

delete_elems_with_score :: proc(
	skip_list: ^SkipList,
	score: f64,
	allocator := context.allocator,
) -> int {
	current_elem := skip_list.start_levels[skip_list.instance_max_level]
	if current_elem == nil do return 0

	for l := skip_list.instance_max_level; l >= 0; l -= 1 {
		next := current_elem.next[l]
		prev := current_elem.prev[l]
		smaller := current_elem.score < score
		next_smaller_or_equal := next != nil && next.score <= score
		greater_or_equal := current_elem.score >= score
		prev_greater_or_equal := prev != nil && prev.score >= score
		if smaller && next_smaller_or_equal {
			for smaller && next_smaller_or_equal {
				current_elem = next
				next = current_elem.next[l]
				smaller = current_elem.score < score
				next_smaller_or_equal = next != nil && next.score <= score
			}
		} else if greater_or_equal && prev_greater_or_equal {
			for greater_or_equal && prev_greater_or_equal {
				current_elem = prev
				prev = current_elem.prev[l]
				greater_or_equal = current_elem.score >= score
				prev_greater_or_equal = prev != nil && prev.score >= score
			}
		}
	}
	count := 0
	for current_elem != nil && current_elem.score == score {
		count += 1
		println(current_elem.score)
		next_elem := current_elem.next[0]
		delete_current_elem(skip_list, current_elem)
		current_elem = next_elem
	}
	return count
}

delete_current_elem :: proc(skip_list: ^SkipList, elem: ^Element, allocator := context.allocator) {
	for l := elem.level; l >= 0; l -= 1 {
		predecessor := elem.prev[l]
		successor := elem.next[l]
		if predecessor == nil && successor == nil {
			skip_list.start_levels[l] = nil
			skip_list.end_levels[l] = nil
			skip_list.instance_max_level -= 1
		} else if predecessor == nil {
			skip_list.start_levels[l] = successor
			successor.prev[l] = nil
		} else if successor == nil {
			skip_list.end_levels[l] = predecessor
			predecessor.next[l] = nil
		} else {
			predecessor.next[l] = successor
			successor.prev[l] = predecessor
		}
	}
	free(elem, allocator)
	skip_list.element_count -= 1
}

getdel_lowest_scores :: proc(skip_list: ^SkipList, count: int = 1) -> []ResultElem {
	res_count := min(count, skip_list.element_count)
	results := make([]ResultElem, res_count)
	current_item := skip_list.start_levels[0]
	for i in 0 ..< res_count {
		next := current_item.next[0]

		results[i] = ResultElem {
			value = current_item.value,
			score = current_item.score,
		}
		delete_current_elem(skip_list, current_item)

		if next == nil do break
		current_item = next
	}
	return results
}

update_score :: proc(
	skip_list: ^SkipList,
	value: string,
	score: f64,
	new_score: f64,
) -> (
	update_count: int,
) {
	update_count = delete_elems_with_value_and_score(skip_list, value, score)
	for i in 0 ..< update_count {
		insert_item(skip_list, value, new_score)
	}
	return update_count
}

getdel_highest_scores :: proc(skip_list: ^SkipList, count: int = 1) -> []ResultElem {
	res_count := min(count, skip_list.element_count)
	results := make([]ResultElem, res_count)
	current_item := skip_list.end_levels[0]
	for i in 0 ..< res_count {
		prev := current_item.prev[0]

		results[i] = ResultElem {
			value = current_item.value,
			score = current_item.score,
		}
		delete_current_elem(skip_list, current_item)

		if prev == nil do break
		current_item = prev
	}
	return results
}

debug_skip_list :: proc(skip_list: SkipList) {
	println("")
	for l in 0 ..= skip_list.instance_max_level {
		println("\nCURRENT LEVEL:", l)
		current_elem := skip_list.start_levels[l]
		for current_elem != nil {
			println("elem", current_elem.value, current_elem.score)
			current_elem = current_elem.next[l]
		}

	}
	println("\ncount:", skip_list.element_count)
	println("")
}

ScoreSet :: struct {
	key_to_score: ^map[string]f64,
	score_to_key: ^SkipList,
	allocator:    mem.Allocator,
}

score_set_init :: proc(allocator := context.allocator) -> ^ScoreSet {
	score_set := new(ScoreSet, allocator)
	score_set^ = ScoreSet {
		key_to_score = new(map[string]f64, allocator),
		score_to_key = new(SkipList, allocator),
		allocator    = allocator,
	}
	return score_set
}

insert_or_update :: proc(score_set: ^ScoreSet, value: string, score: f64) {
	current_score, ok := score_set.key_to_score[value]
	score_set.key_to_score[value] = score
	if ok {
		update_score(score_set.score_to_key, value, current_score, score)
	} else {
		insert_item(score_set.score_to_key, value, score)
	}
}

pop_head :: proc(score_set: ^ScoreSet, count := 1) {
	lowest_scored_items := getdel_lowest_scores(score_set.score_to_key, count)
	for item in lowest_scored_items {
		delete_key(score_set.key_to_score, item.value)
	}
}
