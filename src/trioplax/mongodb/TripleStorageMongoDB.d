module trioplax.mongodb.TripleStorageMongoDB;

private import std.string;
private import std.c.string;

version (D1)
{
private import std.stdio;
}

version (D2)
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

private import trioplax.memory.TripleStorageMemory;
private import trioplax.memory.TripleHashMap;
private import trioplax.memory.IndexException;

class TripleStorageMongoDB: TripleStorage
{
	string query_log_filename = "triple-storage-io";
	private FILE* query_log = null;

	private long total_count_queries = 0;
	private long count_queries_in_cache = 0;

	private int max_length_pull = 1024 * 10;
	private int average_list_size = 3;

	private int strings_max_length = 0;
	private char* strings = null;
	private int last_used_element_in_strings = 0;

	private Triple* triples = null;

	private int elements_in_list_max_length = 0;
	private triple_list_element* elements_in_list = null;
	private int last_used_element_in_pull = 0;

	private triple_list_element*[] used_list = null;

	private char[] buff = null;
	private char* col = cast (char*)"az1";
	private char* ns = cast (char*)"az1.simple";

	//	private char[][1024] query_of_used_lists;
	//	private char[][triple_list_element*] used_lists_pull;
	//	private int count_used_lists = 0;

	private int count_all_allocated_lists = 0;
	private int max_length_list = 0;
	private int max_use_pull = 0;

	private bool[char[]] predicate_as_multiple;

	private bool log_query = false;

	private mongo_connection conn;

	private TripleStorageMemory cache_query_result = null;
	private HashMap list_query = null;

	private char[] P1;
	private char[] P2;
	private char[] store_predicate_in_list_on_idx_s1ppoo;

	this(char[] host, int port)
	{
		triples = cast(Triple*) calloc(Triple.sizeof, max_length_pull * average_list_size);
		strings_max_length = max_length_pull * average_list_size * 3 * 256;
		strings = cast(char*) calloc(char.sizeof, strings_max_length);

		elements_in_list_max_length = max_length_pull * average_list_size;
		elements_in_list = cast(triple_list_element*) calloc(triple_list_element.sizeof, elements_in_list_max_length);

		used_list = new triple_list_element*[max_length_pull];
		last_used_element_in_pull = 0;

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
		cache_query_result = new TripleStorageMemory(300_000, 5, 10_000_000);
		cache_query_result.set_new_index(idx_name.S, 300_000, 5, 10_000_000);
		cache_query_result.set_new_index(idx_name.O, 300_000, 5, 10_000_000);
		cache_query_result.set_new_index(idx_name.PO, 300_000, 5, 10_000_000);
		cache_query_result.set_new_index(idx_name.SP, 300_000, 6, 10_000_000);
		cache_query_result.set_new_index(idx_name.S1PPOO, 300_000, 5, 10_000_000);

		cache_query_result.set_log_query_mode(log_query);

		list_query = new HashMap("list_query", 300_000, 10_000_000, 5);
	}

	public void set_log_query_mode(bool on_off)
	{
		log_query = on_off;
	}

	public void release_all_lists()
	{
		last_used_element_in_pull = 0;
		last_used_element_in_strings = 0;

		//		used_lists_pull = null;
		//count_used_lists = 0;

		//		char[][] values = used_lists_pull.values;

		//		for(int i = 0; i < values.length; i++)
		//		{
		//			log.trace("used list of query {}", values[i]);
		//		}

	}

	public void define_predicate_as_multiple(char[] predicate)
	{
		predicate_as_multiple[predicate] = true;

		if(cache_query_result !is null)
			cache_query_result.define_predicate_as_multiple(predicate);

		log.trace("define predicate [{}] as multiple", predicate);
	}

	public bool f_trace_list_pull = true;

	public void list_no_longer_required(triple_list_element* first_element_of_list)
	{
		/*
		 if(first_element_of_list !is null)
		 {
		 if(f_trace_list_pull)
		 {
		 log.trace("list_no_longer_required ({:X4}), length={}", first_element_of_list, used_lists_pull.length);

		 if((first_element_of_list in used_lists_pull) is null)
		 throw new Exception("как так?");

		 used_lists_pull.remove(first_element_of_list);
		 }

		 triple_list_element* list_iterator = first_element_of_list;
		 while(list_iterator !is null)
		 {
		 Triple* triple = list_iterator.triple;

		 if(triple.s !is null)
		 free(triple.s);

		 if(triple.p !is null)
		 free(triple.p);

		 if(triple.o !is null)
		 free(triple.o);

		 free(cast(void*) triple);

		 triple_list_element* tmp = list_iterator;

		 list_iterator = list_iterator.next_triple_list_element;

		 free(cast(void*) tmp);
		 }

		 count_used_lists--;
		 if(f_trace_list_pull)
		 {
		 log.trace ("list_no_longer_required.. ok");
		 }
		 }
		 */
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

		if(cache_query_result !is null)
			cache_query_result.setPredicatesToS1PPOO(P1, P2, store_predicate_in_list_on_idx_s1ppoo);
	}

	private char[] p_rt = cast(char[])"mo/at/acl#rt\0";

	public triple_list_element* getTriplesUseIndexS1PPOO(char* s, char* p, char* o)
	{

		char ss1[];
		char pp1[];
		char oo1[];

		if(s !is null)
			ss1 = fromStringz(s);
		else
			ss1 = cast(char[])"#";

		if(p !is null)
			pp1 = fromStringz(p);
		else
			pp1 = cast(char[])"#";

		if(o !is null)
			oo1 = fromStringz(o);
		else
			oo1 = cast(char[])"#";

		total_count_queries++;

		triple_list_element* list_in_cache = null;

		bool f_is_query_stored = false;

		// проверим, был ли такой запрос закешированн
		//		log.trace("S1PPOO is_query_in_cache? (s=[{}], p=[{}], o=[{}])", ss1, pp1, oo1);
		//		list_query.f_trace_put = true;
		if(list_query !is null)
		{
			int dummy;
			triple_list_element* is_query_in_cache = list_query.get(ss1.ptr, pp1.ptr, oo1.ptr, dummy);
			if(is_query_in_cache !is null)
			{
				//			log.trace("S1PPOO query_is_in_cache (s=[{}], p=[{}], o=[{}])", ss1, pp1, oo1);

				//			list_in_cache = S1PPOO_IDX.get(s, p, o, dummy);
				list_in_cache = cache_query_result.getTriplesUseIndexS1PPOO(s, p, o);

				if(log_query == true)
					logging_query("GET USE INDEX FROM CACHE", s, p, o, list_in_cache);

				return list_in_cache;
			}

			//		bool fS1 = false;

			if(is_query_in_cache is null)
			{
				log.trace("S1PPOO query_is_not_in_cache (s=[{}], p=[{}], o=[{}])", ss1, pp1, oo1);

				try
				{
					list_query.put(ss1, pp1, oo1, null);
					f_is_query_stored = true;
					count_queries_in_cache++;

				} catch(IndexException ex)
				{
					// при первом же сбое в кэше, отключим его к чертям собачьим :)
					cache_query_result = null;
					list_query = null;

					log.trace("S1PPOO query is not add in cache [list_query]: exception: {}", ex.message);
				}
			}
		}

		bson_buffer bb;
		bson b;

		bson_buffer_init(&bb);

		if(s !is null)
			bson_append_string(&bb, cast(char[])"mo/at/acl#tgSsE", p);

		if(p !is null)
		{
			bson_append_string(&bb, cast(char[])"mo/at/acl#eId", o);
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

			char* ts = null;
			char* tp = strings + last_used_element_in_strings;

			last_used_element_in_strings += p_rt.length;

			if(last_used_element_in_strings > strings_max_length)
				throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

			//			char* tp = cast(char*) calloc(byte.sizeof, "mo/at/acl#rt".length + 1);
			strncpy(tp, p_rt.ptr, p_rt.length);
			char* to = null;

			while(bson_iterator_next(&it))
			{

				char* name_key = bson_iterator_key(&it);

				switch(bson_iterator_type(&it))
				{
					case bson_type.bson_string:
					{
						//						log.trace("getTriplesUseIndex #4");
						char* value = bson_iterator_string(&it);
						int len = strlen(value);

						//						printf("(string) \"%s \" %d\n", value, len);

						if(strcmp(name_key, "ss".ptr) == 0)
						{
							ts = strings + last_used_element_in_strings;

							last_used_element_in_strings += len + 1;
							if(last_used_element_in_strings > strings_max_length)
								throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

							//								ts = cast(char*) calloc(byte.sizeof, len + 1);
							strcpy(ts, value);
						}
						else if(strcmp(name_key, "mo/at/acl#rt".ptr) == 0)
						{
							to = strings + last_used_element_in_strings;
							last_used_element_in_strings += len + 1;
							if(last_used_element_in_strings > strings_max_length)
								throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

							//							to = cast(char*) calloc(byte.sizeof, len + 1);
							strcpy(to, value);
						}
						break;
					}

					default:
					break;
				}
			}

			//			next_element = cast(triple_list_element*) calloc(triple_list_element.sizeof, 1);

			next_element = elements_in_list + last_used_element_in_pull;
			next_element.next_triple_list_element = null;

			Triple* triple = triples + last_used_element_in_pull;

			last_used_element_in_pull++;
			if(last_used_element_in_pull > elements_in_list_max_length)
				throw new Exception("pull is overflow");

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

			//			Triple* triple = cast(Triple*) calloc(Triple.sizeof, 1);
			triple.s = ts;
			triple.p = tp;
			triple.o = to;

			next_element.triple = triple;

			if(f_is_query_stored == true)
			{
				//				log.trace("#1");
				//				S1PPOO_IDX.put (ss1, pp1, oo1, triple);
				//				log.trace("#2");
				char[] ss2 = fromStringz(triple.s);

				try
				{
					cache_query_result.addTriple(ss2, P1, pp1);
					cache_query_result.addTriple(ss2, P2, oo1);

					cache_query_result.addTriple(ss2, fromStringz(triple.p), fromStringz(triple.o));
				} catch(Exception ex)
				{
					// при первом же сбое в кэше, отключим его к чертям собачьим :)
					cache_query_result = null;
					list_query = null;
				}

				//				log.trace("S1PPOO cache_query_result.addTriple <{}><{}>\"{}\"", toString(triple.s), toString(triple.p), toString(triple.o));

				//				log.trace("check adding ");
				//				list_in_cache = cache_query_result.getTriplesUseIndexS1PPOO(s, p, o);
				//				if (list_in_cache !is null)
				//				{
				//					log.trace("OK");
				//				}
				//				else
				//				{
				//					throw new Exception (trioplax.mongodb.TripleStorageMongoDB.stringof ~ " check adding FAIL");					
				//				}

			}

			//			log.trace("get #9, list[{:X4}], triple[{:X4}], triple.o[{:X4}]", &list, triple, triple.o);

			//			log.trace("get:result <{}> <{}> \"{}\"", ts, tp, to);
		}

		if(log_query == true)
			logging_query("GET USE INDEX", s, p, o, list);

		mongo_cursor_destroy(cursor);
		bson_destroy(&b);

		if(list !is null && f_trace_list_pull == true)
		{

			//@@@@@
			/*			
			 char ss[];
			 char pp[];
			 char oo[];

			 if(s !is null)
			 ss = toString(s);

			 if(p !is null)
			 pp = toString(p);

			 if(o !is null)
			 oo = toString(o);

			 if(count_used_lists < max_length_pull)
			 {
			 used_lists_pull[list] = "GET USE INDEX S=" ~ ss ~ ", P=" ~ pp ~ ", O=" ~ oo;
			 //				log.trace("get ({:X4}), length={}", list, used_lists_pull.length);

			 //				query_of_used_lists[count_used_lists] = "GET USE INDEX S=" ~ ss ~ ", P=" ~ pp ~ ", O= " ~ oo;
			 //				used_lists_pull[count_used_lists] = list;
			 }

			 if(length_list > max_length_list)
			 max_length_list = length_list;

			 if(used_lists_pull.length > max_use_pull)
			 max_use_pull = used_lists_pull.length;
			 */
			//		count_used_lists++;
			count_all_allocated_lists++;
			//			if(count_all_allocated_lists % 1000 == 0)
			//				print_stat();
		}

		return list;
	}

	public triple_list_element* getTriples(char* s, char* p, char* o)
	{
		int dummy;

		char ss[];
		char pp[];
		char oo[];

		char ss1[];
		char pp1[];
		char oo1[];

		if(s !is null)
		{
			ss = fromStringz(s);
			ss1 = ss;
			//						log.trace("GET TRIPLES #0 len(s)={}", strlen(s));
		}
		else
		{
			ss1 = cast(char[])"#";
		}

		if(p !is null)
		{
			pp = fromStringz(p);
			pp1 = pp;
			//						log.trace("GET TRIPLES #0 len(p)={}", strlen(p));
		}
		else
		{
			pp1 = cast(char[])"#";
		}

		if(o !is null)
		{
			oo = fromStringz(o);
			oo1 = oo;
			//						log.trace("GET TRIPLES #0, len(o)={}", strlen(o));
		}
		else
		{
			oo1 = cast(char[])"#";
		}

		total_count_queries++;

		triple_list_element* list_in_cache = null;

		bool f_is_query_stored = false;

		if(list_query !is null)
		{
			// проверим, был ли такой запрос закешированн
			//		log.trace("is_query_in_cache? (s=[{}], p=[{}], o=[{}])", ss1, pp1, oo1);
			//		list_query.f_trace_put = true;

			triple_list_element* is_query_in_cache = list_query.get(ss1.ptr, pp1.ptr, oo1.ptr, dummy);
			if(is_query_in_cache !is null)
			{
				//							log.trace("query_is_in_cache (s=[{}], p=[{}], o=[{}])", ss1, pp1, oo1);

				list_in_cache = cache_query_result.getTriples(s, p, o);

				if(log_query == true)
					logging_query("GET FROM CACHE", s, p, o, list_in_cache);

				//				log.trace("list_in_cache={:X8}", list_in_cache);
				return list_in_cache;
			}

			if(is_query_in_cache is null)
			{
				//				log.trace("query_is_not_in_cache (s=[{}], p=[{}], o=[{}])", ss1, pp1, oo1);

				try
				{
					list_query.put(ss1, pp1, oo1, null);
					f_is_query_stored = true;
					count_queries_in_cache++;
				} catch(IndexException ex)
				{
					// при первом же сбое в кэше, отключим его к чертям собачьим :)
					cache_query_result = null;
					list_query = null;

					log.trace("query is not add in cache [list_query]: exception: {}", ex.message);
				}
			}
		}
		//		log.trace("total_count_queries={}, count_queries_in_cache={}", total_count_queries, count_queries_in_cache);

		//		log.trace("GET TRIPLES <{}> <{}> \"{}\"", ss, pp, oo);

		bson_buffer bb, bb2;
		bson query;
		bson fields;

		{
			bson_buffer_init(&bb2);
			bson_buffer_init(&bb);

			if(s !is null)
			{
				bson_append_string(&bb, cast(char[])"ss", s);
				//							bson_append_int(&bb2, "ss", 1);
			}

			if(p !is null && o !is null)
			{
				bson_append_stringz(&bb, p, o);
				//							bson_append_int(&bb2, "pp", 1);
			}

			//		log.trace("GET TRIPLES #4");
			bson_from_buffer(&query, &bb);
			bson_from_buffer(&fields, &bb2);

		}

		//		log.trace("GET TRIPLES #5");
		triple_list_element* list = null;
		triple_list_element* next_element = null;
		triple_list_element* prev_element = null;

		int length_list = 0;

		//		log.trace("GET TRIPLES #6");
		mongo_cursor* cursor = null;
		cursor = mongo_find(&conn, ns, &query, &fields, 0, 0, 0);

		//		log.trace("GET TRIPLES #7");
		while(mongo_cursor_next(cursor))
		{
			bson_iterator it;
			bson_iterator_init(&it, cursor.current.data);

			char* ts = null;
			char* tp = null;
			char* to = null;

			//			log.trace("GET TRIPLES #8");

			while(bson_iterator_next(&it))
			{

				char* name_key = bson_iterator_key(&it);

				switch(bson_iterator_type(&it))
				{
					case bson_type.bson_string:

						char* value = bson_iterator_string(&it);
						int len = strlen(value);

						//						if(len > 0)
						{
							//							log.trace("name_key=[{}], value=[{}], len={}", toString(name_key), toString(value), len);

							if(strcmp(name_key, "ss".ptr) == 0)
							{
								//								ts = cast(char*) calloc(byte.sizeof, len + 1);
								ts = strings + last_used_element_in_strings;
								last_used_element_in_strings += len + 1;
								if(last_used_element_in_strings > strings_max_length)
									throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

								strcpy(ts, value);
							}
							else if(p !is null && strcmp(name_key, p) == 0)
							{
								//								to = cast(char*) calloc(byte.sizeof, len + 1);
								to = strings + last_used_element_in_strings;
								last_used_element_in_strings += len + 1;
								if(last_used_element_in_strings > strings_max_length)
									throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

								strcpy(to, value);
							}
							else if(p is null)
							{
								//								ts = cast(char*) calloc(byte.sizeof, strlen(s) + 1);
								ts = strings + last_used_element_in_strings;
								last_used_element_in_strings += strlen(s) + 1;
								if(last_used_element_in_strings > strings_max_length)
									throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

								strcpy(ts, s);

								//								tp = cast(char*) calloc(byte.sizeof, strlen(name_key) + 1);
								tp = strings + last_used_element_in_strings;
								last_used_element_in_strings += strlen(name_key) + 1;
								if(last_used_element_in_strings > strings_max_length)
									throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

								strcpy(tp, name_key);

								//								to = cast(char*) calloc(byte.sizeof, len + 1);
								to = strings + last_used_element_in_strings;
								last_used_element_in_strings += len + 1;
								if(last_used_element_in_strings > strings_max_length)
									throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

								strcpy(to, value);

								if(ts !is null && tp !is null && to !is null)
								{
									//									next_element = cast(triple_list_element*) calloc(triple_list_element.sizeof, 1);
									next_element = elements_in_list + last_used_element_in_pull;
									next_element.next_triple_list_element = null;

									Triple* triple = triples + last_used_element_in_pull;

									last_used_element_in_pull++;
									if(last_used_element_in_pull > elements_in_list_max_length)
										throw new Exception("pull is overflow");

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

									//									log.trace ("list={:X8}, next_element={:X8}, last_used_element_in_pull={}", list, next_element, last_used_element_in_pull);  
									//			log.trace("GET TRIPLES #10");

									//									Triple* triple = cast(Triple*) calloc(Triple.sizeof, 1);
									//									log.trace ("new triple, ballance={}", ballanse);

									triple.s = ts;
									triple.p = tp;
									triple.o = to;

									next_element.triple = triple;

									if(f_is_query_stored == true)
									{
										try
										{
											cache_query_result.addTriple(fromStringz(triple.s), fromStringz(triple.p),
													fromStringz(triple.o));

										} catch(IndexException ex)
										{
											// при первом же сбое в кэше, отключим его к чертям собачьим :)
											cache_query_result = null;
											list_query = null;
										}
										//										log.trace("cache_query_result.addTriple");
									}
									//			log.trace("get #11, list[{:X4}], triple[{:X4}]", list, triple);

									//									log.trace("get:result <{}> <{}> \"{}\"", toString(ts), toString(tp),
									//											toString(to));
								}
							}
						}

					break;
					/*
					 case bson_type.bson_array:

					 bson_iterator sub_it;
					 bson_iterator_subiterator(&it, &sub_it);

					 while(bson_iterator_next(&sub_it))
					 {
					 switch(bson_iterator_type(&sub_it))
					 {
					 case bson_type.bson_string:

					 char* value = bson_iterator_string(&sub_it);
					 int len = strlen(value);

					 if(len > 0)
					 {
					 //										log.trace("sub:name_key=[{}], value=[{}], len={}", toString(name_key),
					 //												toString(value), len);
					 }

					 break;

					 default:
					 break;
					 }

					 }

					 break;
					 */
					default:
					break;
				}
			}

			if(p !is null)
			{
				//				tp = cast(char*) calloc(byte.sizeof, strlen(p) + 1);
				tp = strings + last_used_element_in_strings;
				last_used_element_in_strings += strlen(p) + 1;
				if(last_used_element_in_strings > strings_max_length)
					throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

				strcpy(tp, p);

				if(o !is null)
				{
					//					to = cast(char*) calloc(byte.sizeof, strlen(o) + 1);
					to = strings + last_used_element_in_strings;
					last_used_element_in_strings += strlen(o) + 1;
					if(last_used_element_in_strings > strings_max_length)
						throw new Exception(trioplax.mongodb.TripleStorageMongoDB.stringof ~ " string area is overflow");

					strcpy(to, o);
				}

				if(ts !is null && tp !is null && to !is null)
				{
					//					log.trace("GET TRIPLES #9");

					//					next_element = cast(triple_list_element*) calloc(triple_list_element.sizeof, 1);
					next_element = elements_in_list + last_used_element_in_pull;
					next_element.next_triple_list_element = null;

					Triple* triple = triples + last_used_element_in_pull;

					last_used_element_in_pull++;
					if(last_used_element_in_pull > elements_in_list_max_length)
						throw new Exception("pull is overflow");

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

					//					Triple* triple = cast(Triple*) calloc(Triple.sizeof, 1);
					triple.s = ts;
					triple.p = tp;
					triple.o = to;

					//					log.trace ("new triple, ballance={}", ballanse);

					next_element.triple = triple;

					if(f_is_query_stored == true)
					{

						try
						{
							cache_query_result.addTriple(fromStringz(triple.s), fromStringz(triple.p), fromStringz(triple.o));
						} catch(IndexException ex)
						{
							// при первом же сбое в кэше, отключим его к чертям собачьим :)
							cache_query_result = null;
							list_query = null;
						}
						//						log.trace("cache_query_result.addTriple");
					}

					//			log.trace("get #11, list[{:X4}], triple[{:X4}]", list, triple);

					//					log.trace("get:result <{}> <{}> \"{}\"", toString(ts), toString(tp), toString(to));
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
			/*
			 if(count_used_lists < max_length_pull)
			 {
			 used_lists_pull[list] = "GET S=" ~ ss ~ ", P=" ~ pp ~ ", O=" ~ oo;
			 //				log.trace("get ({:X4}), length={}", list, used_lists_pull.length);
			 //				query_of_used_lists[count_used_lists] = "GET S=" ~ ss ~ ", P=" ~ pp ~ ", O= " ~ oo;
			 //				used_lists_pull[count_used_lists] = list;
			 }

			 if(length_list > max_length_list)
			 max_length_list = length_list;

			 if(used_lists_pull.length > max_use_pull)
			 max_use_pull = used_lists_pull.length;
			 */
			//		count_used_lists++;
			count_all_allocated_lists++;
			//			if(count_all_allocated_lists % 1000 == 0)
			//				print_stat();
		}

		//		if(f_is_query_stored == true)
		//		{
		//			cache_query_result.print_stat();
		//		}		

		//		log.trace("list={:X8}", list);
		return list;
	}

	private void logging_query(string op, char* s, char* p, char* o, triple_list_element* list)
	{
		char[] a_s = cast(char[])"";
		char[] a_p = cast(char[])"";
		char[] a_o = cast(char[])"";

		if(s !is null)
			a_s = cast(char[])"S";

		if(p !is null)
			a_p = cast(char[])"P";

		if(o !is null)
			a_o = cast(char[])"O";

		int count = get_count_form_list_triple(list);

		if (query_log is null)
			query_log = fopen(cast(char*)query_log_filename.ptr, "w");

//		auto tm = WallClock.now;
//		auto dt = Clock.toDate(tm);
//		log_file.output.write(layout("{:yyyy-MM-dd HH:mm:ss},{} ", tm, dt.time.millis));

fprintf(query_log,"\t%.*s\n", s);

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

		bson_append_string(&bb, cast(char[])"ss", s.ptr);
		bson_append_string(&bb, p, o.ptr);
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
			throw new Exception("remove triple <" ~ cast(string)s ~ "><" ~ cast(string)p ~ ">\"" ~ cast(string)o ~ "\": triple not found");
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
			bson_append_string(&bb, cast(char[])"ss", s.ptr);
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

		if(cache_query_result !is null)
			cache_query_result.removeTriple(s, p, o);

		if(log_query == true)
			logging_query("REMOVE", s.ptr, p.ptr, o.ptr, null);

		return true;
	}

	bool f_trace_addTriple = false;

	public int addTriple(char[] s, char[] p, char[] o)
	{
		//		log.trace("TripleStorageMongoDB:add triple <" ~ s ~ "><" ~ p ~ ">\"" ~ o ~ "\"");
		bson_buffer bb;

		bson op;
		bson cond;

		bson_buffer_init(&bb);
		bson_append_string(&bb, cast(char[])"ss", s.ptr);
		bson_from_buffer(&cond, &bb);

		if((p in predicate_as_multiple) !is null)
		{
			bson_buffer_init(&bb);
			bson_buffer* sub = bson_append_start_object(&bb, "$addToSet");
			bson_append_string(sub, p, o.ptr);
			bson_append_finish_object(sub);
			bson_from_buffer(&op, &bb);
		}
		else
		{
			bson_buffer_init(&bb);
			bson_buffer* sub = bson_append_start_object(&bb, "$set");
			bson_append_string(sub, p, o.ptr);
			
			bson_append_finish_object(sub);
			bson_from_buffer(&op, &bb);
		}

		mongo_update(&conn, ns, &cond, &op, 1);

		bson_destroy(&cond);
		bson_destroy(&op);

		if(cache_query_result !is null)
		{
			cache_query_result.addTriple(s, p, o);
		}

		//		log.trace("TripleStorage:add Triple..ok");

		if(log_query == true)
			logging_query("ADD", s.ptr, p.ptr, o.ptr, null);

		return 0;
	}

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

		log.trace("triple: <{}><{}>\"{}\"", triple.s, triple.p, triple.o);
	}

	public string triple_to_string(Triple* triple)
	{
		if(triple is null)
			return "";

		return "<" ~ cast(string)fromStringz(triple.s) ~ "> <" ~ cast(string)fromStringz(triple.p) ~ "> \"" ~ cast(string)fromStringz(triple.o) ~ "\".\n";
	}

}

char[] fromStringz(char *s)
{
    return s ? s[0 .. strlen(s)] : null;
}

