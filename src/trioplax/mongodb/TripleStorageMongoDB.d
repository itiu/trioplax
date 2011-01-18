module trioplax.mongodb.TripleStorageMongoDB;

private import std.string;
private import std.c.string;
private import std.datetime;
private import std.stdio;

private import std.outbuffer;

version(D1)
{
	private import std.stdio;
}

version(D2)
{
	private import core.stdc.stdio;
	private import core.thread;
}

private import std.c.stdlib: calloc, free;

private import Integer = tango.text.convert.Integer;

private import trioplax.triple;
private import trioplax.TripleStorage;
private import trioplax.Logger;

private import bson;
private import md5;
private import mongo;

Logger log;

static this()
{
	log = new Logger("trioplax.log", "");
}

class TripleStorageMongoDB: TripleStorage
{
	string query_log_filename = "triple-storage-io";
	private FILE* query_log = null;

	private long total_count_queries = 0;

	//	private int max_length_pull = 1024 * 10;
	//	private int average_list_size = 3;

	//	private int max_used_of_triples_pull = 0;
	//	private Triple* triples = null;
	//	private int last_used_of_triples_pull = 0;

	//	private int max_used_of_list_pull = 0;
	//	private triple_list_element* elements_in_list = null;
	//	private int last_used_of_list_pull = 0;

	//	private triple_list_element*[] used_list = null;

	private char[] buff = null;
	private char* col = cast(char*) "coll1";
	private char* ns = cast(char*) "coll1.simple";

	private int count_all_allocated_lists = 0;
	private int max_length_list = 0;
	private int max_use_pull = 0;

	private bool[char[]] predicate_as_multiple;

	private bool log_query = false;

	private mongo_connection conn;

	private char[] P1;
	private char[] P2;
	private char[] store_predicate_in_list_on_idx_s1ppoo;

	char[][] myCreatedString;
	int count_of_myCreatedString;
	int max_of_myCreatedString = 200_000;

	this(string host, int port, string collection)
	{
		multilang_predicates["swrc:name"] = true;
		multilang_predicates["swrc:firstName"] = true;
		multilang_predicates["swrc:lastName"] = true;
		multilang_predicates["gost19:middleName"] = true;
		multilang_predicates["docs19:position"] = true;

		myCreatedString = new char[][max_of_myCreatedString];
		count_of_myCreatedString = 0;

		for(int i = 0; i < myCreatedString.length; i++)
			myCreatedString[i] = new char[255];
		//		myCreatedString[] = new char[255];

		col = cast(char*) collection;
		ns = cast(char*) (collection ~ ".simple");

		//		max_used_of_triples_pull = max_length_pull;
		//		triples = cast(Triple*) calloc(Triple.sizeof, last_used_of_triples_pull);
		//		last_used_of_triples_pull = 0;

		//		max_used_of_list_pull = max_length_pull * average_list_size;
		//		elements_in_list = cast(triple_list_element*) calloc(triple_list_element.sizeof, max_used_of_list_pull);
		//		last_used_of_list_pull = 0;

		//		used_list = new triple_list_element*[max_length_pull];

		//		layout = new Locale;
		buff = new char[32];

		mongo_connection_options opts;

		strncpy(cast(char*) opts.host, host.ptr, 255);
		opts.host[254] = '\0';
		opts.port = port;

		if(mongo_connect(&conn, &opts))
		{
			log.trace("failed to connect to mongodb");
			throw new Exception("failed to connect to mongodb");
		}
		log.trace("connect to mongodb sucessful");
	}

	public void set_cache()
	{
	}

	public void set_log_query_mode(bool on_off)
	{
		log_query = on_off;
	}

	public void release_all_lists()
	{
		//		last_used_of_list_pull = 0;
		//		last_used_of_triples_pull = 0;
	}

	public void define_predicate_as_multiple(char[] predicate)
	{
		predicate_as_multiple[cast(immutable) predicate] = true;

		log.trace("define predicate [{}] as multiple", predicate);
	}

	public bool f_trace_list_pull = true;

	public void list_no_longer_required(triple_list_element* first_element_of_list)
	{
	}

	public void set_new_index(ubyte index, uint max_count_element, uint max_length_order, uint inital_triple_area_length)
	{
	}

	public void set_stat_info_logging(bool flag)
	{
	}

	public void setPredicatesToS1PPOO(char[] _P1, char[] _P2, char[] _store_predicate_in_list_on_idx_s1ppoo)
	{
		P1 = _P1;
		P2 = _P2;
		store_predicate_in_list_on_idx_s1ppoo = _store_predicate_in_list_on_idx_s1ppoo;
	}

	public bool isExistSubject(char[] subject)
	{
		StopWatch sw;
		sw.start();

		bool res = false;

		bson_buffer bb, bb2;
		bson query;
		bson fields;

		{
			bson_buffer_init(&bb2);
			bson_buffer_init(&bb);

			if(subject !is null)
			{
				bson_append_stringA(&bb, cast(char[]) "@", subject);
			}

			bson_from_buffer(&query, &bb);
			bson_from_buffer(&fields, &bb2);
		}

		mongo_cursor* cursor = null;
		cursor = mongo_find(&conn, ns, &query, &fields, 0, 0, 0);

		if(mongo_cursor_next(cursor))
		{
			res = true;
		}

		mongo_cursor_destroy(cursor);
		bson_destroy(&fields);
		bson_destroy(&query);

		sw.stop();
		long t = cast(long) sw.peek().microseconds;

		if(t > 500)
		{
			log.trace("isExistSubject [%s], total time: %d[µs]", subject, t);
		}
		return res;
	}

	public triple_list_element getTriples(char[] s, char[] p, char[] o)
	{
		StopWatch sw;
		sw.start();

		int dummy;

		total_count_queries++;

		//		triple_list_element* list_in_cache = null;

		bool f_is_query_stored = false;

		bson_buffer bb, bb2;
		bson query;
		bson fields;

		{
			bson_buffer_init(&bb2);
			bson_buffer_init(&bb);

			if(s !is null)
			{
				bson_append_stringA(&bb, cast(char[]) "@", s);
			}

			if(p !is null && o !is null)
			{
				bson_append_stringA(&bb, p, o);
			}

			//			bson_append_int(&bb2, cast(char*)"@", 1);
			//			if (p !is null)
			//			{
			//				bson_append_stringA(&bb2, p, cast(char[]) "1");
			//			}

			//		log.trace("GET TRIPLES #4");
			bson_from_buffer(&fields, &bb2);
			bson_from_buffer(&query, &bb);

		}

		//		log.trace("GET TRIPLES #5");
		triple_list_element list = null;
		triple_list_element last_element = null;

		int length_list = 0;

		//		log.trace("GET TRIPLES #6");
		mongo_cursor* cursor = null;
		cursor = mongo_find(&conn, ns, &query, &fields, 0, 0, 0);

		while(mongo_cursor_next(cursor))
		{
			bson_iterator it;
			bson_iterator_init(&it, cursor.current.data);

			char[] ts = null;
			char[] tp = null;
			char[] to = null;

			//			printf("GET TRIPLES #8\n");

			while(bson_iterator_next(&it))
			{

				switch(bson_iterator_type(&it))
				{
					case bson_type.bson_string:

						char[] name_key = fromStringz(bson_iterator_key(&it));
						//						writeln("name_key=[", name_key, "]");

						char[] value = fromStringz(bson_iterator_string(&it));
						//						writeln(" value=[", value, "]");
						{

							if(name_key == "@")
							{
								//								writeln("ts = value");
								ts = value;
							}

							else if(p !is null && name_key == p)
							{
								//								writeln("to = value");
								tp = name_key;
								to = value;
							}

							if(p is null)
							{
								tp = name_key;
								to = value;
							}

							if(ts !is null && tp !is null && to !is null)
							{
								add_triple_in_list(ts, tp, to, last_element, list);
							}
						}

					break;
					default:
					break;
				}
			}

			if(p !is null)
			{

				if(ts !is null && tp !is null && to !is null)
				{
					add_triple_in_list(s, p, o, last_element, list);
				}
			}
		}

		mongo_cursor_destroy(cursor);
		bson_destroy(&fields);
		bson_destroy(&query);

		//		if(log_query == true)
		//			logging_query("GET", s, p, o, list);

		if(list !is null && f_trace_list_pull == true)
		{
			count_all_allocated_lists++;
		}

		//		log.trace("list={:X8}", list);
		sw.stop();
		long t = cast(long) sw.peek().microseconds;

		if(t > 500)
		{
			log.trace("query-> [%s][%s][%s], total time getTriple: %d[µs] ", s, p, o, t);
		}

		return list;
	}

	private void logging_query(string op, char[] s, char[] p, char[] o, triple_list_element* list)
	{
		log.trace("%s [%s] [%s] [%s]", op, s, p, o);
	}

	public bool removeTriple(char[] s, char[] p, char[] o)
	{
		//		log.trace("TripleStorageMongoDB:remove triple <" ~ s ~ "><" ~ p ~ ">\"" ~ o ~ "\"");

		if(s is null || p is null || o is null)
		{
			throw new Exception("remove triple:s is null || p is null || o is null");
		}

		//		log.trace("remove! #1");

		bson_buffer bb;
		bson query;
		bson fields;
		//		bson record;

		bson_buffer_init(&bb);
		//		log.trace("remove! #2");

		bson_append_stringA(&bb, cast(char[]) "@", s);
		bson_append_stringA(&bb, p, o);
		bson_from_buffer(&query, &bb);
		mongo_cursor* cursor = mongo_find(&conn, ns, &query, &fields, 0, 0, 0);

		//		log.trace("remove! #3");

		if(mongo_cursor_next(cursor))
		{
			bson_iterator it;
			bson_iterator_init(&it, cursor.current.data);
			switch(bson_iterator_type(&it))
			{
				case bson_type.bson_string:

					log.trace("remove! string");

				break;

				case bson_type.bson_array:

					log.trace("remove! array");

				break;

				default:
				break;
			}

		}
		else
		{
			throw new Exception(
					"remove triple <" ~ cast(string) s ~ "><" ~ cast(string) p ~ ">\"" ~ cast(string) o ~ "\": triple not found");
		}

		mongo_cursor_destroy(cursor);
		bson_destroy(&fields);
		bson_destroy(&query);

		//		bson_buffer bb;
		//		bson b;
		{

			bson op;
			bson cond;

			bson_buffer_init(&bb);
			bson_append_stringA(&bb, cast(char[]) "@", s);
			bson_from_buffer(&cond, &bb);

			//			if(p == HAS_PART)
			//			{
			//				bson_buffer_init(&bb);
			//				bson_buffer* sub = bson_append_start_object(&bb,
			//						"$pull");
			//				bson_append_int(sub, p.ptr, 1);
			//				bson_append_finish_object(sub);
			//			} else
			{
				bson_buffer_init(&bb);
				bson_buffer* sub = bson_append_start_object(&bb, "$unset");
				bson_append_int(sub, p.ptr, 1);
				bson_append_finish_object(sub);
			}

			bson_from_buffer(&op, &bb);
			mongo_update(&conn, ns, &cond, &op, 0);

			bson_destroy(&cond);
			bson_destroy(&op);
		}

		//		if(cache_query_result !is null)
		//			cache_query_result.removeTriple(s, p, o);

		if(log_query == true)
			logging_query("REMOVE", s, p, o, null);

		return true;
	}

	public void addTripleToReifedData(char[] reif_subject, char[] reif_predicate, char[] reif_object, char[] p, char[] o, byte lang = _NONE)
	{
		//  {SUBJECT:[$reif_subject]}{$set: {'_reif_[$reif_predicate].[$reif_object].[$p]' : [$o]}});

		p = "_reif_" ~ reif_predicate ~ "." ~ reif_object ~ "." ~ p ~ "";

		addTriple(reif_subject, p, o, lang);

		return;
	}

	public int addTriple(char[] s, char[] p, char[] o, byte lang = _NONE)
	{
		//				trace_msg[4] = 1;

		StopWatch sw;
		sw.start();

		if(trace_msg[4][1] == 1)
			logging_query("ADD", s, p, o, null);

		//		if(trace_msg[4][0] == 1)
		//			log.trace("TripleStorageMongoDB:add triple <" ~ s ~ "><" ~ p ~ ">\"" ~ o ~ "\" lang=", lang);

		bson_buffer bb;

		bson op;
		bson cond;

		bson_buffer_init(&bb);
		bson_append_stringA(&bb, cast(char[]) "@", s);
		bson_from_buffer(&cond, &bb);

		bson_buffer_init(&bb);
		if((p in predicate_as_multiple) !is null)
		{
			bson_buffer* sub = bson_append_start_object(&bb, "$addToSet");

			if(lang == _NONE)
				bson_append_stringA(sub, p, o);
			else if(lang == _RU)
				bson_append_stringA(sub, p, o ~ "@ru");
			if(lang == _EN)
				bson_append_stringA(sub, p, o ~ "@en");

			bson_append_finish_object(sub);
		}
		else
		{
			bson_buffer* sub;

			if(lang == _NONE)
			{
				sub = bson_append_start_object(&bb, "$set");
				bson_append_stringA(sub, p, o);
			}
			else if(lang == _RU)
			{
				sub = bson_append_start_object(&bb, "$addToSet");
				bson_append_stringA(sub, p, o ~ "@ru");
			}
			else if(lang == _EN)
			{
				sub = bson_append_start_object(&bb, "$addToSet");
				bson_append_stringA(sub, p, o ~ "@en");
			}

			bson_append_finish_object(sub);
		}

		// добавим данные для полнотекстового поиска

		{
			bson_buffer* sub = bson_append_start_object(&bb, "$addToSet");

			char[] l_o = tolower(o);

			if(lang == _NONE)
				bson_append_stringA(sub, cast(char[]) "_keywords", l_o);
			else if(lang == _RU)
				bson_append_stringA(sub, cast(char[]) "_keywords", l_o ~ "@ru");
			if(lang == _EN)
				bson_append_stringA(sub, cast(char[]) "_keywords", l_o ~ "@en");

			bson_append_finish_object(sub);
		}

		bson_from_buffer(&op, &bb);

		//		Thread.getThis().sleep(100_000);

		mongo_update(&conn, ns, &cond, &op, 1);

		bson_destroy(&cond);
		bson_destroy(&op);

		sw.stop();
		long t = cast(long) sw.peek().microseconds;

		if(t > 300 || trace_msg[4][2] == 1)
		{
			log.trace("total time add triple: %d[µs]", cast(long) sw.peek().microseconds);
		}

		return 0;
	}

	//
	public void print_stat()
	{
		log.trace("TripleStorage:stat: max used pull={}, max length list={}", max_use_pull, max_length_list);

		//		char[][] values = used_lists_pull.values;

		//		for(int i = 0; i < values.length; i++)
		//		{
		//			log.trace("used list of query {}", values[i]);
		//		}
	}

	/*
	 public void print_list_triple_to_file(File log_file, triple_list_element* list_iterator)
	 {
	 Triple* triple;
	 if(list_iterator !is null)
	 {
	 while(list_iterator !is null)
	 {
	 //				log.trace("#KKK {:X4} {:X4} {:X4}", list_iterator, *list_iterator, *(list_iterator + 1));

	 triple = list_iterator.triple;
	 if(triple !is null)
	 {
	 char[] triple_str = triple_to_string(triple);
	 log_file.output.write(triple_str);
	 }

	 list_iterator = list_iterator.next_triple_list_element;
	 }
	 }
	 }
	 */
	public void print_list_triple(triple_list_element list_iterator)
	{
		writeln("=== begin list");

		Triple triple;
		if(list_iterator !is null)
		{
			while(list_iterator !is null)
			{
				//				log.trace("#KKK {:X4} {:X4} {:X4}", list_iterator, *list_iterator, *(list_iterator + 1));

				triple = list_iterator.triple;
				if(triple !is null)
					print_triple(triple);

				list_iterator = list_iterator.next_triple_list_element;
			}
		}
		writeln("=== end list");
	}

	public int get_count_form_list_triple(triple_list_element list_iterator)
	{
		int count = 0;
		Triple triple;
		if(list_iterator !is null)
		{
			while(list_iterator !is null)
			{
				triple = list_iterator.triple;
				if(triple !is null)
				{
					count++;
				}

				list_iterator = list_iterator.next_triple_list_element;

			}
		}
		return count;
	}

	public void print_triple(Triple triple)
	{
		if(triple is null)
			return;

		writeln("triple: ", triple.s, " ", triple.p, " ", triple.o);
	}

	public string triple_to_string(Triple triple)
	{
		if(triple is null)
			return "";

		return cast(string) ("<" ~ triple.s ~ "> <" ~ triple.p ~ "> \"" ~ triple.o ~ "\".\n");
	}

	byte trace_msg[10][30];

	bool[char[]] multilang_predicates;

	private void add_fulltext_to_query(char[] fulltext_param, bson_buffer* bb)
	{
		bson_buffer* sub = bson_append_start_object(bb, "_keywords");

		bson_buffer* sub1 = bson_append_start_array(bb, "$all");

		char[][] values = split(fulltext_param, ",");
		foreach(val; values)
		{
			bson_append_regexA(sub1, null, val, null);
		}

		bson_append_finish_object(sub1);

		bson_append_finish_object(sub);
	}

	public triple_list_element getTriplesOfMask(ref Triple[] mask_triples, byte[char[]] reading_predicates)
	{
		//		trace_msg[0] = 1;
		//trace_msg[0][5] = 1;
		//		trace_msg[1][1] = 0;
		//		trace_msg[2][0] = 0;

		int count_of_reifed_data = 0;

		StopWatch sw;
		sw.start();

		if(trace_msg[0][1] == 1)
			log.trace("getTriplesOfMask START mask_triples.length=%d\n", mask_triples.length);

		try
		{
			triple_list_element list = null;
			triple_list_element last_element = null;

			bson_buffer bb;
			bson_buffer bb2;
			bson fields;
			bson query;

			bson_buffer_init(&bb2);
			bson_buffer_init(&bb);

			//			bson_append_stringA(&bb2, cast(char[]) "@", cast(char[]) "1");

			for(short i = 0; i < mask_triples.length; i++)
			{
				if(trace_msg[0][2] == 1)
					log.trace("getTriplesOfMask i=%d", i);

				char[] s = mask_triples[i].s;
				char[] p = mask_triples[i].p;
				char[] o = mask_triples[i].o;

				if(s !is null && s.length > 0)
				{
					add_to_query(cast(char[]) "@", s, &bb);
				}

				if(p !is null && p == "query:fulltext")
				{
					add_fulltext_to_query(o, &bb);
				}
				else if(p !is null && o !is null && o.length > 0)
				{
					add_to_query(p, o, &bb);
				}
			}

			reading_predicates["@"] = _GET;

			//			int count_readed_fields = 0;
			//			for(int i = 0; i < reading_predicates.keys.length; i++)
			//			{
			//				char[] field_name = cast(char[]) reading_predicates.keys[i];
			//				byte field_type = reading_predicates.values[i];
			//				bson_append_stringA(&bb2, cast(char[]) field_name, cast(char[]) "1");
			//
			//				if(trace_msg[0][3] == 1)
			//					log.trace("getTriplesOfMask:set out field:%s", field_name);
			//
			//				if(field_type == _GET_REIFED)
			//				{
			//					bson_append_stringA(&bb2, cast(char[]) "_reif_" ~ field_name, cast(char[]) "1");
			//
			//					if(trace_msg[0][4] == 1)
			//						log.trace("getTriplesOfMask:set out field:%s", "_reif_" ~ field_name);
			//				}
			//
			//				count_readed_fields++;
			//			}

			bson_from_buffer(&fields, &bb2);

			bson_from_buffer(&query, &bb);

			if(trace_msg[0][5] == 1)
			{
				char[] ss = bson_to_string(&query);
				log.trace("getTriplesOfMask:QUERY:\n %s", ss);
				//				log.trace("---- readed fields=%s", reading_predicates);
			}

			if(mask_triples.length == 0)
			{
				if(trace_msg[0][22] == 1)
					log.trace("getTriplesOfMask:mask_triples.length == 0, return");

				return null;
			}

			StopWatch sw0;
			sw0.start();

			mongo_cursor* cursor = mongo_find(&conn, ns, &query, &fields, 0, 0, 0);

			sw0.stop();
			long t0 = cast(long) sw0.peek().microseconds;

			if(t0 > 100)
			{
				char[] ss = bson_to_string(&query);
				log.trace("getTriplesOfMask:QUERY:\n %s", ss);

				log.trace("getTripleOfMask: mongo_find: %d[µs]", t0);
			}

			char[] S;
			char[] P;
			char[] O;

			Triple[][char[]] reif_triples;

			while(mongo_cursor_next(cursor))
			{
				if(trace_msg[0][6] == 1)
					log.trace("getTriplesOfMask:next of cursor");

				bson_iterator it;
				bson_iterator_init(&it, cursor.current.data);

				short count_fields = 0;
				while(bson_iterator_next(&it))
				{
					//					writeln ("it++");
					bson_type type = bson_iterator_type(&it);
					if(trace_msg[0][7] == 1)
						log.trace("getTriplesOfMask:next key, TYPE=%d", type);

					switch(type)
					{
						case bson_type.bson_string:
						{
							char[] _name_key = fromStringz(bson_iterator_key(&it));

							byte* type_of_getting_field = (_name_key in reading_predicates);

							if(type_of_getting_field is null)
								break;

							if(trace_msg[0][8] == 1)
								log.trace("getTriplesOfMask:_name_key:%s", _name_key);

							char[] _value = fromStringz(bson_iterator_string(&it));

							if(trace_msg[0][9] == 1)
								log.trace("getTriplesOfMask:_value:%s", _value);

							if(_name_key == "@")
							{
								S = _value;
							}
							else if(_name_key[0] != '_')
							{
								P = _name_key;
								O = _value;

								// проверим есть ли для этого триплета реифицированные данные
								if(*type_of_getting_field == _GET_REIFED)
								{
									Triple[]* vv = O in reif_triples;
									if(vv !is null)
									{
										Triple[] r1_reif_triples = *vv;

										add_triple_in_list(r1_reif_triples[0].s, cast(char[]) "rdf:Subject", S, last_element, list);
										add_triple_in_list(r1_reif_triples[0].s, cast(char[]) "rdf:Predicate", P, last_element, list);
										add_triple_in_list(r1_reif_triples[0].s, cast(char[]) "rdf:Object", O, last_element, list);

										foreach(tt; r1_reif_triples)
										{
											// можно добавлять в список
											if(trace_msg[0][10] == 1)
												log.trace("getTriplesOfMask:можно добавлять в список :", tt.o);

											add_triple_in_list(tt, last_element, list);
										}
									}
								}
								add_triple_in_list(S, P, O, last_element, list);
							}
							//							else if(_name_key[1] != 'r' && _name_key[2] != 'e' && _name_key[3] != 'i')
							//							{
							//								if(trace_msg[0][11] == 1)
							//									log.trace("getTriplesOfMask:REIF _name_key:%s", _name_key);
							//							}

							break;
						}

						case bson_type.bson_array:
						{
							char[] _name_key = fromStringz(bson_iterator_key(&it));

							if(_name_key != "@" && _name_key[0] != '_')
							{
								if(trace_msg[0][12] == 1)
									log.trace("getTriplesOfMask:_name_key:%s", _name_key);

								char* val = bson_iterator_value(&it);

								bson_iterator i_1;
								bson_iterator_init(&i_1, val);

								while(bson_iterator_next(&i_1))
								{
									switch(bson_iterator_type(&i_1))
									{
										case bson_type.bson_string:
										{
											char[] A_value = fromStringz(bson_iterator_string(&i_1));

											add_triple_in_list(S, _name_key, A_value, last_element, list);
										}
										default:
										break;
									}

								}
							}
							break;
						}

						case bson_type.bson_object:
						{
							char[] _name_key = fromStringz(bson_iterator_key(&it));

							if(_name_key[0] == '_' && _name_key[1] == 'r' && _name_key[2] == 'e' && _name_key[3] == 'i')
							{
								count_of_reifed_data++;

								char[] reifed_data_subj = new char[6];
								reifed_data_subj[0] = '_';
								reifed_data_subj[1] = ':';
								reifed_data_subj[2] = 'R';
								reifed_data_subj[3] = '_';
								reifed_data_subj[4] = '_';
								reifed_data_subj[5] = '_';

								Integer.format(reifed_data_subj, count_of_reifed_data, cast(char[]) "X2");

								// это реифицированные данные, восстановим факты его образующие
								// добавим в список:
								//	_new_node_uid a fdr:Statement
								//	_new_node_uid rdf:subject [$S]
								//	_new_node_uid rdf:predicate [$_name_key[6..]]
								//	_new_node_uid rdf:object [?]

								Triple[] r_triples = new Triple[10];
								int last_r_triples = 0;

								if(trace_msg[0][13] == 1)
									log.trace("getTriplesOfMask:REIFFF _name_key:%s", _name_key);

								char* val = bson_iterator_value(&it);
								bson_iterator i_L1;
								bson_iterator_init(&i_L1, val);

								while(bson_iterator_next(&i_L1))
								{

									switch(bson_iterator_type(&i_L1))
									{

										case bson_type.bson_object:
										{
											char[] _name_key_L1 = fromStringz(bson_iterator_key(&i_L1));
											if(trace_msg[0][14] == 1)
												log.trace("getTriplesOfMask:_name_key_L1 %s", _name_key_L1);

											char* val_L2 = bson_iterator_value(&i_L1);

											bson_iterator i_L2;
											bson_iterator_init(&i_L2, val_L2);

											while(bson_iterator_next(&i_L2))
											{
												switch(bson_iterator_type(&i_L2))
												{
													case bson_type.bson_string:
													{
														Triple r_triple = new Triple;
														r_triples[last_r_triples] = r_triple;
														last_r_triples++;
														if(last_r_triples > r_triples.length)
															r_triples.length += 50;

														char[] _name_key_L2 = fromStringz(bson_iterator_key(&i_L2));

														if(trace_msg[0][15] == 1)
															log.trace("getTriplesOfMask:_name_key_L2=%s", _name_key_L2);

														r_triple.p = _name_key_L2;

														char[] _name_val_L2 = fromStringz(bson_iterator_string(&i_L2));

														if(trace_msg[0][16] == 1)
															log.trace("getTriplesOfMask:_name_val_L2L=%s", _name_val_L2);

														r_triple.o = _name_val_L2;

														r_triple.s = reifed_data_subj;

														break;
													}

													default:
													break;
												}
											}

											r_triples.length = last_r_triples;
											reif_triples[cast(immutable) _name_key_L1] = r_triples;

											break;
										}
										/*
										 case bson_type.bson_eoo:
										 {
										 char[] _name_val_L1 = fromStringz(bson_iterator_string(&i_L1));

										 if(trace_msg[0][18] == 1)
										 log.trace("getTriplesOfMask:bson_type.bson_eoo QQQ L1 VAL=%s", _name_val_L1);

										 r_triples.length = last_r_triples;
										 reif_triples[cast(immutable) _name_key_L1] = r_triples;

										 break;
										 }
										 */
										default:
										break;
									}

								}
							}

							break;
						}

						default:
							{
								if(trace_msg[0][19] == 1)
								{
									char[] _name_key = fromStringz(bson_iterator_key(&it));
									log.trace("getTriplesOfMask:_name_key:", _name_key);
								}
							}
						break;

					}
				}

			}

			mongo_cursor_destroy(cursor);
			//			bson_destroy(&fields);

			sw.stop();
			long t = cast(long) sw.peek().microseconds;

			if(t > 100 || trace_msg[0][20] == 1)
			{
				char[] ss = bson_to_string(&query);
				log.trace("getTriplesOfMask:QUERY:\n %s", ss);

				log.trace("total time getTripleOfMask: %d[µs]", t);
			}

			if(trace_msg[0][21] == 1)
				print_list_triple(list);

			bson_destroy(&query);

			return list;
		}
		catch(Exception ex)
		{
			log.trace("@exception:%s", ex.msg);
			throw ex;
		}

	}

	//	private Triple* createTriple()
	//	{
	//		Triple* triple = triples + last_used_of_triples_pull;

	//		last_used_of_triples_pull++;
	//		if(last_used_of_triples_pull > max_used_of_triples_pull)
	//			throw new Exception("triple pull is overflow, last_used_of_triples_pull > max_used_of_triples_pull");

	//		return triple;
	//}

	private void add_triple_in_list(char[] S, char[] P, char[] O, ref triple_list_element last_added_element, ref triple_list_element list)
	{
		//		trace_msg[1] = 1;
		if(trace_msg[1][0] == 1)
			log.trace("add_triple_in_list last_added_element=%s", last_added_element);

		// добавим триплет в возвращаемый список
		triple_list_element next_element = new triple_list_element;

		if(list is null)
			list = next_element;

		next_element.next_triple_list_element = null;

		Triple triple = new Triple;

		if(last_added_element !is null)
		{
			last_added_element.next_triple_list_element = next_element;
		}

		triple.s = S;
		triple.p = P;
		char[][] o_tags = std.string.split(O, "@");

		if(o_tags[].length > 1)
		{
			triple.o = o_tags[0];

			if(o_tags[1] == "ru")
				triple.lang = _RU;
			if(o_tags[1] == "en")
				triple.lang = _EN;
		}
		else
		{
			triple.o = O;
			triple.lang = _NONE;
		}

		if(trace_msg[1][1] == 1)
			log.trace("add_triple_in_list S:%s P:%s O:%s lang:%d", triple.s, triple.p, triple.o, triple.lang);

		next_element.triple = triple;

		if(trace_msg[1][2] == 1)
			log.trace("add_triple_in_list return");

		last_added_element = next_element;
	}

	private void add_triple_in_list(Triple triple, ref triple_list_element last_added_element, ref triple_list_element list)
	{
		// добавим триплет в возвращаемый список
		triple_list_element next_element = new triple_list_element;
		if(list is null)
			list = next_element;

		next_element.next_triple_list_element = null;

		if(last_added_element !is null)
		{
			last_added_element.next_triple_list_element = next_element;
		}

		if(trace_msg[2][0] == 1)
			log.trace("add_triple_in_list S:%s P:%s O:%s lang:%d", triple.s, triple.p, triple.o, triple.lang);

		next_element.triple = triple;
		last_added_element = next_element;
	}

	char[] fromStringz(char* s)
	{
		//		printf ("count_of_myCreatedString=%d\n", count_of_myCreatedString);
		//		char[] res = s ? s[0 .. strlen(s)] : null;

		// для того чтоб GC не уничтожил созданные строки внутри методов этого класса, 
		// складируем созданные char[] в массив экземпляра класса.   

		int len = strlen(s);
		char[] res = myCreatedString[count_of_myCreatedString];

		res.length = len;
		strncpy(res.ptr, s, len);
		count_of_myCreatedString++;

		if(count_of_myCreatedString >= max_of_myCreatedString)
		{
			count_of_myCreatedString = 0;
		}

		return res;
	}

	char[] fromStringz(char* s, int len)
	{
		//		printf ("count_of_myCreatedString=%d\n", count_of_myCreatedString);
		//		char[] res = s ? s[0 .. len] : null;

		// для того чтоб GC не уничтожил созданные строки внутри методов этого класса, 
		// складируем созданные char[] в массив экземпляра класса.   

		char[] res = myCreatedString[count_of_myCreatedString];

		res.length = len;
		strncpy(res.ptr, s, len);
		count_of_myCreatedString++;

		if(count_of_myCreatedString >= max_of_myCreatedString)
		{
			count_of_myCreatedString = 0;
		}

		return res;
	}

	private void add_to_query(char[] field_name, char[] field_value, bson_buffer* bb)
	{
		if(trace_msg[3][0] == 1)
			log.trace("add_to_query ^^^ field_name = %s, field_value=%s", field_name, field_value);

		bool field_is_multilang = (field_name in multilang_predicates) !is null;

		if(field_value !is null && (field_value[0] == '"' && field_value[1] == '[' || field_value[0] == '['))
		{
			if(field_value[0] == '[')
				field_value = field_value[1 .. field_value.length - 1];
			else
				field_value = field_value[2 .. field_value.length - 2];

			char[][] values = split(field_value, ",");
			if(values.length > 0)
			{
				bson_buffer* sub = bson_append_start_array(bb, "$or");

				foreach(val; values)
				{
					bson_buffer* sub1 = bson_append_start_object(bb, "");

					if(field_is_multilang)
						bson_append_stringA(sub1, field_name, val ~ "@ru");
					else
						bson_append_stringA(sub1, field_name, val);

					bson_append_finish_object(sub1);
				}

				bson_append_finish_object(sub);
			}
		}
		else
		{
			if(field_is_multilang)
			{
				bson_append_stringA(bb, field_name, field_value ~ "@ru");
			}
			else
				bson_append_stringA(bb, cast(char[]) field_name, field_value);
		}

		if(trace_msg[3][1] == 1)
			log.trace("add_to_query return");
	}
}

char[] getString(char* s)
{
	return s ? s[0 .. strlen(s)] : null;
}

char[] bson_to_string(bson* b)
{
	OutBuffer outbuff = new OutBuffer();
	bson_raw_to_string(b.data, 0, outbuff);
	outbuff.write(0);
	return getString(cast(char*) outbuff.toBytes());
}

void bson_raw_to_string(char* data, int depth, OutBuffer outbuff)
{
	bson_iterator i;
	char* key;
	int temp;
	char oidhex[25];
	bson_iterator_init(&i, data);

	while(bson_iterator_next(&i))
	{
		bson_type t = bson_iterator_type(&i);
		if(t == 0)
			break;

		key = bson_iterator_key(&i);

		for(temp = 0; temp <= depth; temp++)
			outbuff.write(cast(char[]) "\t");

		outbuff.write(getString(key));
		outbuff.write(cast(char[]) ":");

		switch(t)
		{
			case bson_type.bson_int:
				outbuff.write(cast(char[]) "int ");
				outbuff.write(bson_iterator_int(&i));
			break;

			case bson_type.bson_double:
				outbuff.write(cast(char[]) "double ");
				outbuff.write(bson_iterator_double(&i));
			break;

			case bson_type.bson_bool:
				outbuff.write(cast(char[]) "bool ");
				outbuff.write((bson_iterator_bool(&i) ? cast(char[]) "true" : cast(char[]) "false"));
			break;

			case bson_type.bson_string:
				outbuff.write(cast(char[]) "string ");
				outbuff.write(getString(bson_iterator_string(&i)));
			break;

			case bson_type.bson_regex:
				outbuff.write(cast(char[]) "regex ");
				outbuff.write(getString(bson_iterator_regex(&i)));
			break;

			case bson_type.bson_null:
				outbuff.write(cast(char[]) "null");
			break;

			//			case bson_type.bson_oid:
			//				bson_oid_to_string(bson_iterator_oid(&i), cast(char*) &oidhex);
			//				printf("%s", oidhex);
			//			break; //@@@ cast (char*)&oidhex)
			case bson_type.bson_object:
			case bson_type.bson_array:
				outbuff.write(cast(char[]) "\n");
				bson_raw_to_string(bson_iterator_value(&i), depth + 1, outbuff);
			break;
			//			default:
			//				fprintf(stderr, "can't print type : %d\n", t);
		}
		outbuff.write(cast(char[]) "\n");
	}
}
