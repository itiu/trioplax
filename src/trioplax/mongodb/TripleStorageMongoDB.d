module trioplax.mongodb.TripleStorageMongoDB;

private import std.string;
private import std.c.string;
private import std.datetime;
private import std.stdio;

version(D1)
{
	private import std.stdio;
}

version(D2)
{
	private import core.stdc.stdio;
}

private import std.c.stdlib: calloc, free;

private import trioplax.triple;
private import trioplax.TripleStorage;
private import trioplax.Log;

private import bson;
private import md5;
private import mongo;

class TripleStorageMongoDB: TripleStorage
{
	string query_log_filename = "triple-storage-io";
	private FILE* query_log = null;

	private long total_count_queries = 0;

	private int max_length_pull = 1024 * 10;
	private int average_list_size = 3;

	private int max_used_of_triples_pull = 0;
	private Triple* triples = null;
	private int last_used_of_triples_pull = 0;

	private int max_used_of_list_pull = 0;
	private triple_list_element* elements_in_list = null;
	private int last_used_of_list_pull = 0;

	private triple_list_element*[] used_list = null;

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
	int max_of_myCreatedString = 100_000;

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

		max_used_of_triples_pull = max_length_pull;
		triples = cast(Triple*) calloc(Triple.sizeof, last_used_of_triples_pull);
		last_used_of_triples_pull = 0;

		max_used_of_list_pull = max_length_pull * average_list_size;
		elements_in_list = cast(triple_list_element*) calloc(triple_list_element.sizeof, max_used_of_list_pull);
		last_used_of_list_pull = 0;

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
		last_used_of_list_pull = 0;
		last_used_of_triples_pull = 0;
	}

	public void define_predicate_as_multiple(char[] predicate)
	{
		predicate_as_multiple[predicate] = true;

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

	private char[] p_rt = cast(char[]) "mo/at/acl#rt\0";

	public triple_list_element* getTriplesUseIndexS1PPOO(char[] s, char[] p, char[] o)
	{
		total_count_queries++;

		triple_list_element* list_in_cache = null;

		bool f_is_query_stored = false;

		bson_buffer bb;
		bson b;

		bson_buffer_init(&bb);

		if(s !is null)
			bson_append_stringA(&bb, cast(char[]) "mo/at/acl#tgSsE", p);

		if(p !is null)
		{
			bson_append_stringA(&bb, cast(char[]) "mo/at/acl#eId", o);
		}

		bson_from_buffer(&b, &bb);

		mongo_cursor* cursor = mongo_find(&conn, ns, &b, null, 0, 0, 0);

		//		log.trace("getTriplesUseIndex #2");

		triple_list_element* list = null;
		triple_list_element* next_element = null;
		triple_list_element* prev_element = null;

		int length_list = 0;

		while(mongo_cursor_next(cursor))
		{
			//			log.trace("getTriplesUseIndex #3");
			bson_iterator it;
			bson_iterator_init(&it, cursor.current.data);

			char[] ts = null;
			char[] tp = null;

			tp = p_rt;

			char[] to = null;

			while(bson_iterator_next(&it))
			{

				char[] name_key = fromStringz(bson_iterator_key(&it));

				switch(bson_iterator_type(&it))
				{
					case bson_type.bson_string:
					{
						//						log.trace("getTriplesUseIndex #4");
						char[] value = fromStringz(bson_iterator_string(&it));
						//						int len = strlen(value);

						//						printf("(string) \"%s \" %d\n", value, len);

						if(name_key == "SUBJECT")
						{
							ts = value;
						}
						else if(name_key == "mo/at/acl#rt")
						{
							to = value;
						}
						break;
					}

					default:
					break;
				}
			}

			//			next_element = cast(triple_list_element*) calloc(triple_list_element.sizeof, 1);

			next_element = elements_in_list + last_used_of_list_pull;
			next_element.next_triple_list_element = null;

			last_used_of_list_pull++;
			if(last_used_of_list_pull > max_used_of_list_pull)
				throw new Exception("list elements pull is overflow");

			Triple* triple = triples + last_used_of_triples_pull;

			last_used_of_triples_pull++;
			if(last_used_of_triples_pull > max_used_of_triples_pull)
				throw new Exception("triples pull is overflow");

			length_list++;

			if(prev_element !is null)
			{
				prev_element.next_triple_list_element = next_element;
			}

			prev_element = next_element;
			if(list is null)
			{
				//				log.trace("getTriplesUseIndex #1 [{}] [{}] [{}]", toString(s), toString(p), toString(o));
				list = next_element;
			}
			//			log.trace ("list={:X8}, next_element={:X8}, last_used_element_in_pull={}", list, next_element, last_used_element_in_pull);  

			triple.s = ts;
			triple.p = tp;
			triple.o = to;

			next_element.triple = triple;
		}

		if(log_query == true)
			logging_query("GET USE INDEX", s, p, o, list);

		mongo_cursor_destroy(cursor);
		bson_destroy(&b);

		if(list !is null && f_trace_list_pull == true)
		{
			count_all_allocated_lists++;
		}

		return list;
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
				bson_append_stringA(&bb, cast(char[]) "SUBJECT", subject);
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

		if(t > 100)
		{
			writeln("Subject:", subject);
			printf("total time isExistSubject: %d[µs]\n", t);
		}
		return res;
	}

	public triple_list_element* getTriples(char* s, char* p, char* o)
	{
		return getTriples(fromStringz(s), fromStringz(p), fromStringz(o));
	}

	public triple_list_element* getTriples(char[] s, char[] p, char[] o)
	{
		StopWatch sw;
		sw.start();

		int dummy;

		total_count_queries++;

		triple_list_element* list_in_cache = null;

		bool f_is_query_stored = false;

		bson_buffer bb, bb2;
		bson query;
		bson fields;

		{
			bson_buffer_init(&bb2);
			bson_buffer_init(&bb);

			if(s !is null)
			{
				bson_append_stringA(&bb, cast(char[]) "SUBJECT", s);
			}

			if(p !is null && o !is null)
			{
				bson_append_stringA(&bb, p, o);
			}

			//			bson_append_int(&bb2, cast(char*)"SUBJECT", 1);
			//			if (p !is null)
			//			{
			//				bson_append_stringA(&bb2, p, cast(char[]) "1");
			//			}

			//		log.trace("GET TRIPLES #4");
			bson_from_buffer(&fields, &bb2);
			bson_from_buffer(&query, &bb);

		}

		//		log.trace("GET TRIPLES #5");
		triple_list_element* list = null;
		triple_list_element* next_element = null;
		triple_list_element* prev_element = null;

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

							if(name_key == "SUBJECT")
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
								//								writeln("if(ts !is null && tp !is null && to !is null)");

								//	next_element = cast(triple_list_element*) calloc(triple_list_element.sizeof, 1);
								next_element = elements_in_list + last_used_of_list_pull;
								next_element.next_triple_list_element = null;

								last_used_of_list_pull++;
								if(last_used_of_list_pull > max_used_of_list_pull)
									throw new Exception("list elements pull is overflow");

								Triple* triple = triples + last_used_of_triples_pull;

								last_used_of_triples_pull++;
								if(last_used_of_triples_pull > max_used_of_triples_pull)
									throw new Exception("triples pull is overflow");

								if(prev_element !is null)
								{
									prev_element.next_triple_list_element = next_element;
								}

								prev_element = next_element;
								if(list is null)
								{
									//										log.trace("getTriples [{}] [{}] [{}]", toString(s), toString(p), toString(o));
									list = next_element;
								}

								triple.s = ts;
								triple.p = tp;
								triple.o = to;

								next_element.triple = triple;
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
					//					log.trace("GET TRIPLES #9");

					//					next_element = cast(triple_list_element*) calloc(triple_list_element.sizeof, 1);
					next_element = elements_in_list + last_used_of_list_pull;
					next_element.next_triple_list_element = null;

					last_used_of_list_pull++;
					if(last_used_of_list_pull > max_used_of_list_pull)
						throw new Exception("list elements pull is overflow");

					Triple* triple = triples + last_used_of_triples_pull;

					last_used_of_triples_pull++;
					if(last_used_of_triples_pull > max_used_of_triples_pull)
						throw new Exception("triples pull is overflow");

					length_list++;

					if(prev_element !is null)
					{
						prev_element.next_triple_list_element = next_element;
					}

					prev_element = next_element;
					if(list is null)
					{
						//						log.trace("getTriples [{}] [{}] [{}]", toString(s), toString(p), toString(o));
						list = next_element;
					}
					//					log.trace ("list={:X8}, next_element={:X8}, last_used_element_in_pull={}", list, next_element, last_used_element_in_pull);  

					//			log.trace("GET TRIPLES #10");

					triple.s = s;
					triple.p = p;
					triple.o = o;

					//					log.trace ("new triple, ballance={}", ballanse);

					next_element.triple = triple;
				}
			}
		}

		mongo_cursor_destroy(cursor);
		bson_destroy(&fields);
		bson_destroy(&query);

		if(log_query == true)
			logging_query("GET", s, p, o, list);

		if(list !is null && f_trace_list_pull == true)
		{
			count_all_allocated_lists++;
		}

		//		log.trace("list={:X8}", list);
		sw.stop();
		long t = cast(long) sw.peek().microseconds;

		if(t > 100)
		{
			writeln("query-> S:", s, " P:", p, " O:", o);
			printf("total time getTriple: %d[µs]\n", t);
		}

		return list;
	}

	private void logging_query(string op, char[] s, char[] p, char[] o, triple_list_element* list)
	{
		char[] a_s = cast(char[]) "";
		char[] a_p = cast(char[]) "";
		char[] a_o = cast(char[]) "";

		if(s !is null)
			a_s = cast(char[]) "S";

		if(p !is null)
			a_p = cast(char[]) "P";

		if(o !is null)
			a_o = cast(char[]) "O";

		int count = get_count_form_list_triple(list);

		if(query_log is null)
			query_log = fopen(cast(char*) query_log_filename.ptr, "w");

		//		auto tm = WallClock.now;
		//		auto dt = Clock.toDate(tm);
		//		log_file.output.write(layout("{:yyyy-MM-dd HH:mm:ss},{} ", tm, dt.time.millis));

		fprintf(query_log, "\t%.*s\n", s);

		//		log_file.output.write(op ~ "\n s=[" ~ toString(s) ~ "] p=[" ~ toString(p) ~ "] o=[" ~ toString(o) //~ "] " ~ Integer.format(
		//				buff, count) ~ "\n");

		//		print_list_triple_to_file(log_file, list);

		//		log_file.close();

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

		bson_append_stringA(&bb, cast(char[]) "SUBJECT", s);
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
			bson_append_stringA(&bb, cast(char[]) "SUBJECT", s);
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

	bool f_trace_addTripleToReifedData = true;

	public void addTripleToReifedData(char[] reif_subject, char[] reif_predicate, char[] reif_object, char[] p, char[] o, byte lang = _NONE)
	{
		//  {SUBJECT:[$reif_subject]}{$set: {'_reif_[$reif_predicate].[$reif_object].[$p]' : [$o]}});

		p = "_reif_" ~ reif_predicate ~ "." ~ reif_object ~ "." ~ p ~ "";

		addTriple(reif_subject, p, o, lang);

		return;
	}

	bool f_trace_addTriple = false;

	public int addTriple(char[] s, char[] p, char[] o, byte lang = _NONE)
	{
		StopWatch sw;
		sw.start();

		if(f_trace_addTriple)
			writeln("TripleStorageMongoDB:add triple <" ~ s ~ "><" ~ p ~ ">\"" ~ o ~ "\" lang=", lang);

		bson_buffer bb;

		bson op;
		bson cond;

		bson_buffer_init(&bb);
		bson_append_stringA(&bb, cast(char[]) "SUBJECT", s);
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

		mongo_update(&conn, ns, &cond, &op, 1);

		bson_destroy(&cond);
		bson_destroy(&op);

		if(log_query == true)
			logging_query("ADD", s, p, o, null);

		sw.stop();
		long t = cast(long) sw.peek().microseconds;

		if(t > 10)
		{
			printf("total time add triple: %d[µs]\n", cast(long) sw.peek().microseconds);
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
	public void print_list_triple(triple_list_element* list_iterator)
	{
		writeln("=== begin list");

		Triple* triple;
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

	public int get_count_form_list_triple(triple_list_element* list_iterator)
	{
		int count = 0;
		Triple* triple;
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

	public void print_triple(Triple* triple)
	{
		if(triple is null)
			return;

		writeln("triple: ", triple.s, " ", triple.p, " ", triple.o);
	}

	public string triple_to_string(Triple* triple)
	{
		if(triple is null)
			return "";

		return cast(string) ("<" ~ triple.s ~ "> <" ~ triple.p ~ "> \"" ~ triple.o ~ "\".\n");
	}

	bool trace__getTriplesOfMask = false;
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

	private void add_to_query(char[] field_name, char[] field_value, bson_buffer* bb)
	{
		if(trace__getTriplesOfMask)
			writeln("^^^ field_name = ", field_name, ", field_value=", field_value);

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

	}

	bool trace__getTriplesOfMask_1 = false;
	bool trace__getTriplesOfMask_2 = false;
	bool trace__getTriplesOfMask_3 = false;
	bool trace__getTriplesOfMask_4 = false;
	bool trace__getTriplesOfMask_5 = true;
	bool trace__getTriplesOfMask_6 = false;
	bool trace__getTriplesOfMask_7 = true;
	bool trace__getTriplesOfMask_8 = true;
	bool trace__getTriplesOfMask_9 = false;
	bool trace__getTriplesOfMask_10 = false;
	bool trace__getTriplesOfMask_11 = true;
	bool trace__getTriplesOfMask_12 = false;
	bool trace__getTriplesOfMask_13 = false;

	public triple_list_element* getTriplesOfMask(ref Triple[] mask_triples, byte[char[]] reading_predicates)
	{
		StopWatch sw;
		sw.start();

		if(trace__getTriplesOfMask_1)
			printf("getTriplesOfMask\n");

		try
		{
			triple_list_element* list = null;
			triple_list_element* next_element = null;
			triple_list_element* prev_element = null;

			bson_buffer bb;
			bson_buffer bb2;
			bson fields;
			bson query;

			bson_buffer_init(&bb2);
			bson_buffer_init(&bb);

			bson_append_stringA(&bb2, cast(char[]) "SUBJECT", cast(char[]) "1");

			for(short i = 0; i < mask_triples.length; i++)
			{
				char[] s = mask_triples[i].s;
				char[] p = mask_triples[i].p;
				char[] o = mask_triples[i].o;

				if(s !is null && s.length > 0)
				{
					add_to_query(cast(char[]) "SUBJECT", s, &bb);
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

			int count_readed_fields = 0;
			for(int i = 0; i < reading_predicates.keys.length; i++)
			{
				char[] field_name = cast(char[]) reading_predicates.keys[i];
				byte field_type = reading_predicates.values[i];
				bson_append_stringA(&bb2, cast(char[]) field_name, cast(char[]) "1");

				if(trace__getTriplesOfMask_2)
					writeln("getTriplesOfMask:set out field:", field_name);

				if(field_type == _GET_REIFED)
				{
					bson_append_stringA(&bb2, cast(char[]) "_reif_" ~ field_name, cast(char[]) "1");

					if(trace__getTriplesOfMask_3)
						writeln("getTriplesOfMask:set out field:", "_reif_" ~ field_name);
				}

				count_readed_fields++;
			}

			bson_from_buffer(&fields, &bb2);
			bson_from_buffer(&query, &bb);

			if(trace__getTriplesOfMask_4)
			{
				printf("getTriplesOfMask:QUERY:\n");
				bson_print(&query);
			}

			mongo_cursor* cursor = mongo_find(&conn, ns, &query, &fields, 0, 0, 0);

			char[] S;
			char[] P;
			char[] O;

			Triple*[][char[]] reif_triples;

			while(mongo_cursor_next(cursor))
			{
				if(trace__getTriplesOfMask_5)
					writeln("getTriplesOfMask:next of cursor");

				bson_iterator it;
				bson_iterator_init(&it, cursor.current.data);

				short count_fields = 0;
				while(bson_iterator_next(&it))
				{
					//					writeln ("it++");
					bson_type type = bson_iterator_type(&it);
					if(trace__getTriplesOfMask_6)
						printf("getTriplesOfMask:TYPE_OF_KEY %d\n", type);

					switch(type)
					{
						case bson_type.bson_string:
						{
							char[] _name_key = fromStringz(bson_iterator_key(&it));

							if(trace__getTriplesOfMask_7)
								writeln("getTriplesOfMask:_name_key:", _name_key);

							char[] _value = fromStringz(bson_iterator_string(&it));

							if(trace__getTriplesOfMask_8)
								writeln("getTriplesOfMask:_value:", _value);

							if(_name_key == "SUBJECT")
							{
								S = _value;
							}
							else if(_name_key[0] != '_')
							{
								P = _name_key;
								O = _value;

								// проверим есть ли для этого триплета реифицированные данные

								Triple*[]* vv = O in reif_triples;
								if(vv !is null)
								{
									Triple*[] r1_reif_triples = *vv;

									foreach (tt; r1_reif_triples)
									{
										// можно добавлять в список
										writeln("getTriplesOfMask:можно добавлять в список :", tt.o);
										add_triple_in_list(tt, list, next_element, prev_element);
									}
								}

								add_triple_in_list(S, P, O, list, next_element, prev_element);
							}
							else if(_name_key[1] != 'r' && _name_key[2] != 'e' && _name_key[3] != 'i')
							{
								if(trace__getTriplesOfMask_9)
									writeln("getTriplesOfMask:REIF _name_key:", _name_key);
							}

							break;
						}

						case bson_type.bson_array:
						{
							char[] _name_key = fromStringz(bson_iterator_key(&it));

							if(_name_key != "SUBJECT" && _name_key[0] != '_')
							{
								if(trace__getTriplesOfMask_10)
									writeln("getTriplesOfMask:_name_key:", _name_key);

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

											add_triple_in_list(S, _name_key, A_value, list, next_element, prev_element);
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
								// это реифицированные данные, восстановим факты его образующие
								// добавим в список:
								//	_new_node_uid a fdr:Statement
								//	_new_node_uid rdf:subject [$S]
								//	_new_node_uid rdf:predicate [$_name_key[6..]]
								//	_new_node_uid rdf:object [?]

								Triple*[] r_triples = new Triple*[10];
								int last_r_triples = 0;

								if(trace__getTriplesOfMask_11)
									writeln("getTriplesOfMask:REIFFF _name_key:", _name_key);

								char* val = bson_iterator_value(&it);
								bson_iterator i_L1;
								bson_iterator_init(&i_L1, val);

								while(bson_iterator_next(&i_L1))
								{
									char[] _name_key_L1 = fromStringz(bson_iterator_key(&i_L1));

									switch(bson_iterator_type(&i_L1))
									{

										case bson_type.bson_object:
										{
											char* val_L2 = bson_iterator_value(&i_L1);

											bson_iterator i_L2;
											bson_iterator_init(&i_L2, val_L2);

											while(bson_iterator_next(&i_L2))
											{
												switch(bson_iterator_type(&i_L2))
												{
													case bson_type.bson_string:
													{
														Triple* r_triple = createTriple();
														r_triples[last_r_triples] = r_triple;
														last_r_triples++;
														if(last_r_triples > r_triples.length)
															r_triples.length += 50;

														printf("getTriplesOfMask:QQQ L2 %d\n", bson_iterator_type(&i_L2));
														char[] _name_key_L2 = fromStringz(bson_iterator_key(&i_L2));
														writeln("getTriplesOfMask:QQQ L2 KEY ", _name_key_L2);
														r_triple.p = _name_key_L2;

														char[] _name_val_L2 = fromStringz(bson_iterator_string(&i_L2));
														writeln("getTriplesOfMask:QQQ L2 VAL ", _name_val_L2);
														r_triple.o = _name_val_L2;

														r_triple.s = S ~ "_reif";

														break;
													}

													default:
														writeln("getTriplesOfMask:REIFFF #3");

													break;
												}
											}

											break;
										}

										case bson_type.bson_eoo:
										{
											char[] _name_val_L1 = fromStringz(bson_iterator_string(&i_L1));
											writeln("getTriplesOfMask: ", bson_iterator_type(&i_L1), " QQQ L1 VAL ", _name_val_L1);

											r_triples.length = last_r_triples;
											reif_triples[_name_val_L1] = r_triples;

											break;
										}

										default:
										break;
									}

								}
							}

							break;
						}

						default:
							{
								char[] _name_key = fromStringz(bson_iterator_key(&it));

								if(trace__getTriplesOfMask_12)
									writeln("getTriplesOfMask:_name_key:", _name_key);
							}
						break;

					}
				}

			}

			mongo_cursor_destroy(cursor);
			bson_destroy(&query);
			bson_destroy(&fields);

			sw.stop();
			long t = cast(long) sw.peek().microseconds;

			if(t > 100)
			{
				printf("total time getTripleOfMask: %d[µs]\n", t);
			}

			if(trace__getTriplesOfMask_13)
				print_list_triple(list);

			return list;
		}
		catch(Exception ex)
		{
			printf("@@@ Ex\n");
			log.trace("@exception:", ex.msg);
			throw ex;
		}

	}

	private Triple* createTriple()
	{
		Triple* triple = triples + last_used_of_triples_pull;

		last_used_of_triples_pull++;
		if(last_used_of_triples_pull > max_used_of_triples_pull)
			throw new Exception("triple pull is overflow, last_used_of_triples_pull > max_used_of_triples_pull");

		return triple;
	}

	private void add_triple_in_list(char[] S, char[] P, char[] O, ref triple_list_element* list, ref triple_list_element* next_element,
			ref triple_list_element* prev_element)
	{

		// добавим триплет в возвращаемый список
		next_element = elements_in_list + last_used_of_list_pull;
		next_element.next_triple_list_element = null;

		last_used_of_list_pull++;
		if(last_used_of_list_pull > max_used_of_list_pull)
			throw new Exception("list pull is overflow, last_used_of_triples_pull > max_used_of_triples_pull");

		Triple* triple = createTriple();

		if(prev_element !is null)
		{
			prev_element.next_triple_list_element = next_element;
		}

		prev_element = next_element;
		if(list is null)
		{
			list = next_element;
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

		if(trace__getTriplesOfMask)
			writeln("add triple to the list, S:", triple.s, " P:", triple.p, " O:", triple.o, " lang:", triple.lang);

		next_element.triple = triple;
	}

	private void add_triple_in_list(Triple* triple, ref triple_list_element* list, ref triple_list_element* next_element,
			ref triple_list_element* prev_element)
	{
		// добавим триплет в возвращаемый список
		next_element = elements_in_list + last_used_of_list_pull;
		next_element.next_triple_list_element = null;

		last_used_of_list_pull++;
		if(last_used_of_list_pull > max_used_of_list_pull)
			throw new Exception("list pull is overflow, last_used_of_triples_pull > max_used_of_triples_pull");

		if(prev_element !is null)
		{
			prev_element.next_triple_list_element = next_element;
		}

		prev_element = next_element;
		if(list is null)
		{
			list = next_element;
		}

		if(trace__getTriplesOfMask)
			writeln("add Triple* to the list, S:", triple.s, " P:", triple.p, " O:", triple.o, " lang:", triple.lang);

		next_element.triple = triple;
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
}
