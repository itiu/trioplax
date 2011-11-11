// TODO Batch insert/update
// TODO утечка памяти при добавлении фактов
// TODO утечка памяти при чтении фактов

module trioplax.mongodb.TripleStorageMongoDB;

private import std.string;
private import std.c.string;
private import std.datetime;
private import std.stdio;
private import std.outbuffer;
private import core.stdc.stdio;
private import core.thread;

import rt.util.hash;

private import Integer = tango.text.convert.Integer;

private import trioplax.triple;
private import trioplax.TripleStorage;
private import trioplax.Logger;

private import trioplax.memory.TripleStorageMemory;
private import trioplax.memory.ComplexKeys;

private import mongo_h;
private import mongo;
private import bson_h;
private import bson;

private int MAX_SIZE_READ_RECORDS = 1000;

Logger log;

enum caching_type: byte
{
	NONE = 0,
	ALL_DATA = 1,
	QUERY_RESULT = 2
}

static this()
{
	log = new Logger("trioplax", "log", "");
}

class TripleStorageMongoDBIterator: TLIterator
{
	mongo_cursor* cursor;
	byte[char[]] reading_predicates;
	bool is_query_all_predicates = false;
	bool is_get_all = false;
	bool is_get_all_reifed = false;

	TripleStorageMemory ts_in_mem;

	this(mongo_cursor* _cursor, TripleStorageMemory _ts_in_mem = null)
	{
		cursor = _cursor;
		ts_in_mem = _ts_in_mem;
	}

	this(mongo_cursor* _cursor, ref byte[char[]] _reading_predicates, TripleStorageMemory _ts_in_mem = null)
	{
		cursor = _cursor;
		ts_in_mem = _ts_in_mem;
		reading_predicates = _reading_predicates;

		if(reading_predicates.length > 0)
		{
			byte* type_of_getting_field = ("query:all_predicates" in reading_predicates);

			if(type_of_getting_field !is null)
			{
				is_query_all_predicates = true;
				if(*type_of_getting_field == field.GET)
				{
					is_get_all = true;
				} else if(*type_of_getting_field == field.GET_REIFED)
				{
					is_get_all_reifed = true;
				}

			}
		}

	}

	~this()
	{
		if(cursor !is null)
		{
			mongo_cursor_destroy(cursor);
		}
	}

	int opApply(int delegate(ref Triple) dg)
	{
		if(cursor is null)
			return -1;

		int result = 0;

		string S;
		string P;
		string O;

		Triple[][FKeys] reif_triples;
		int count_of_reifed_data = 0;

		if(trace_msg[1007] == 1)
			log.trace("opApply:TripleStorageMongoDBIterator:cursor %x", cursor);

		if(trace_msg[1007] == 1)
			log.trace("opApply:TripleStorageMongoDBIterator:reading_predicates %s", reading_predicates);

		while(mongo_cursor_next(cursor) == MONGO_OK)
		{
			//			log.trace("while(mongo_cursor_next(cursor) == MONGO_OK)");
			bson_iterator it;
			bson_iterator_init(&it, &cursor.current);

			short count_fields = 0;
			while(bson_iterator_next(&it))
			{
				//					writeln ("it++");
				bson_type type = bson_iterator_type(&it);

				if(trace_msg[1007] == 1)
					log.trace("TripleStorageMongoDBIterator:next key, TYPE=%d", type);

				switch(type)
				{
					case bson_type.BSON_STRING:
					{
						string _name_key = fromStringz(bson_iterator_key(&it));

						if(trace_msg[1008] == 1)
							log.trace("TripleStorageMongoDBIterator:string:_name_key:%s", _name_key);

						// если не указанны требуемые предикаты, то берем какие были считанны
						byte* type_of_getting_field = null;
						if(is_query_all_predicates == false && reading_predicates.length > 0)
						{
							type_of_getting_field = (_name_key in reading_predicates);

							if(type_of_getting_field is null)
								break;
						}

						string _value = fromStringz(bson_iterator_string(&it));

						if(trace_msg[1009] == 1)
							log.trace("TripleStorageMongoDBIterator:_value:%s", _value);

						if(_name_key == "@")
						{
							S = _value;
						} else if(_name_key[0] != '_')
						{
							P = _name_key;
							O = _value;

							//\\							TODO !is_query_all_predicates оставлено для совместимости тестов

							//							if (is_query_all_predicates)	
							//							{
							//								Triple tt000 = new Triple(S, P, O);
							//														
							//								result = dg(tt000);
							//								if(result)
							//									return -1;
							//							}
							//							else
							//							{
							if(O !is null)
							// && O.length > 0) 
							{
								Triple tt000 = new Triple(S, P, O);

								result = dg(tt000);
								if(ts_in_mem !is null)
									ts_in_mem.addTriple(tt000);
								if(result)
									return -1;
							}
							//							}

							// проверим есть ли для этого триплета реифицированные данные
							if(is_get_all_reifed == true || type_of_getting_field !is null && *type_of_getting_field == field.GET_REIFED)
							{
								FKeys t3 = new FKeys(S, P, O);

								Triple[]* vv = t3 in reif_triples;
								if(vv !is null)
								{
									Triple[] r1_reif_triples = *vv;

									if(r1_reif_triples !is null && r1_reif_triples.length > 0)
									{
										Triple tt0 = new Triple(r1_reif_triples[0].S, "a", "rdf:Statement");

										result = dg(tt0);
										if(ts_in_mem !is null)
											ts_in_mem.addTriple(tt0);
										if(result)
											return 1;

										tt0 = new Triple(r1_reif_triples[0].S, "rdf:subject", S);

										result = dg(tt0);
										if(ts_in_mem !is null)
											ts_in_mem.addTriple(tt0);
										if(result)
											return 1;

										tt0 = new Triple(r1_reif_triples[0].S, "rdf:predicate", P);
										if(ts_in_mem !is null)
											ts_in_mem.addTriple(tt0);
										result = dg(tt0);
										if(result)
											return 1;

										tt0 = new Triple(r1_reif_triples[0].S, "rdf:object", O);
										if(ts_in_mem !is null)
											ts_in_mem.addTriple(tt0);
										result = dg(tt0);
										if(result)
											return 1;

										foreach(tt; r1_reif_triples)
										{
											// можно добавлять в список
											if(trace_msg[1010] == 1)
												log.trace("reif : %s", tt);

											result = dg(tt);
											if(ts_in_mem !is null)
												ts_in_mem.addTriple(tt);
											if(result)
												return 1;
										}
									}
								}
							}

						}

						break;
					}

					case bson_type.BSON_ARRAY:
					{
						string _name_key = fromStringz(bson_iterator_key(&it));

						if(trace_msg[1008] == 1)
							log.trace("TripleStorageMongoDBIterator:string:_name_key:%s", _name_key);

						if(_name_key != "@" && _name_key[0] != '_')
						{

							// если не указанны требуемые предикаты, то берем какие были считанны
							byte* type_of_getting_field = null;
							if(is_query_all_predicates == false && reading_predicates.length > 0)
							{
								type_of_getting_field = (_name_key in reading_predicates);

								if(type_of_getting_field is null)
									break;
							}

							bson_iterator i_1;
							bson_iterator_subiterator(&it, &i_1);

							while(bson_iterator_next(&i_1))
							{
								switch(bson_iterator_type(&i_1))
								{
									case bson_type.BSON_STRING:
									{
										string A_value = fromStringz(bson_iterator_string(&i_1));

										if(A_value.length > 0)
										{
											Triple tt0 = new Triple(S, _name_key, A_value);
											result = dg(tt0);
											if(ts_in_mem !is null)
												ts_in_mem.addTriple(tt0);
											if(result)
												return 1;
										}
									}
									default:
									break;
								}

							}
						}
						break;
					}

					case bson_type.BSON_OBJECT:
					{
						string _name_key = fromStringz(bson_iterator_key(&it));

						if(trace_msg[1008] == 1)
							log.trace("TripleStorageMongoDBIterator:string:_name_key:%s", _name_key);

						// если не указанны требуемые предикаты, то берем какие были считанны
						byte* type_of_getting_field = null;
						if(is_query_all_predicates == false && reading_predicates.length > 0)
						{
							type_of_getting_field = (_name_key in reading_predicates);

							if(type_of_getting_field is null)
								break;
						}

						if(_name_key[0] == '_' && _name_key[1] == 'r' && _name_key[2] == 'e' && _name_key[3] == 'i')
						{
							string s_reif_parent_triple = S;
							string p_reif_parent_triple = _name_key[6 .. $];

							// это реифицированные данные, восстановим факты его образующие
							// добавим в список:
							//	_new_node_uid a fdr:Statement
							//	_new_node_uid rdf:subject [$S]
							//	_new_node_uid rdf:predicate [$_name_key[6..]]
							//	_new_node_uid rdf:object [?]

							Triple[] r_triples = new Triple[10];
							int last_r_triples = 0;

							//							if(trace_msg[1013] == 1)
							//								log.trace("TripleStorageMongoDBIterator:REIFFF _name_key:%s", _name_key);

							//							char* val = bson_iterator_value(&it);
							bson_iterator i_L1;
							bson_iterator_subiterator(&it, &i_L1);
							//							bson_iterator_init(&i_L1, val);

							while(bson_iterator_next(&i_L1))
							{
								switch(bson_iterator_type(&i_L1))
								{

									case bson_type.BSON_OBJECT:
									{
										count_of_reifed_data++; //???

										char[] reifed_data_subj = new char[7];
										reifed_data_subj[0] = '_';
										reifed_data_subj[1] = ':';
										reifed_data_subj[2] = 'R';
										reifed_data_subj[3] = '_';
										reifed_data_subj[4] = '_';
										reifed_data_subj[5] = '_';
										reifed_data_subj[6] = 0;

										Integer.format(reifed_data_subj, count_of_reifed_data, cast(char[]) "X2");

										//										log.trace("TripleStorageMongoDBIterator: # <, count_of_reifed_data=%s", reifed_data_subj);										

										string _name_key_L1 = fromStringz(bson_iterator_key(&i_L1));
										string o_reif_parent_triple = _name_key_L1;

										if(trace_msg[1014] == 1)
											log.trace("TripleStorageMongoDBIterator:_name_key_L1 %s", _name_key_L1);

										//										char* val_L2 = bson_iterator_value(&i_L1);

										//										log.trace("TripleStorageMongoDBIterator: val_L2 %s", fromStringz(val_L2));

										bson_iterator i_L2;
										//										bson_iterator_init(&i_L2, val_L2);
										bson_iterator_subiterator(&i_L1, &i_L2);

										//										log.trace("TripleStorageMongoDBIterator: # {");

										while(bson_iterator_next(&i_L2))
										{
											switch(bson_iterator_type(&i_L2))
											{
												case bson_type.BSON_STRING:
												{
													string _name_key_L2 = fromStringz(bson_iterator_key(&i_L2));

													if(trace_msg[1015] == 1)
														log.trace("TripleStorageMongoDBIterator:_name_key_L2=%s",
																_name_key_L2);

													string _name_val_L2 = fromStringz(bson_iterator_string(&i_L2));

													if(trace_msg[1016] == 1)
														log.trace("TripleStorageMongoDBIterator:_name_val_L2L=%s",
																_name_val_L2);

													//	r_triple.P = _name_key_L2;
													//	r_triple.O = _name_val_L2;
													//	r_triple.S = cast(immutable) reifed_data_subj;

													Triple r_triple = new Triple(cast(string) reifed_data_subj,
															_name_key_L2, _name_val_L2);
													//													log.trace("++ triple %s", r_triple);

													if(last_r_triples >= r_triples.length)
														r_triples.length += 50;

													r_triples[last_r_triples] = r_triple;

													last_r_triples++;

													break;
												}

												case bson_type.BSON_ARRAY:
												{
													string _name_key_L2 = fromStringz(bson_iterator_key(&i_L2));

													//													val = bson_iterator_value(&i_L2);

													bson_iterator i_1;
													bson_iterator_subiterator(&i_L2, &i_1);
													//													bson_iterator_init(&i_1, val);

													while(bson_iterator_next(&i_1))
													{
														switch(bson_iterator_type(&i_1))
														{
															case bson_type.BSON_STRING:
															{
																string
																		A_value = fromStringz(
																				bson_iterator_string(&i_1));

																Triple r_triple = new Triple(
																		cast(string) reifed_data_subj, _name_key_L2,
																		A_value);

																if(last_r_triples >= r_triples.length)
																	r_triples.length += 50;

																r_triples[last_r_triples] = r_triple;

																last_r_triples++;
															}
															default:
															break;
														}

													}

													break;
												}

												default:
												break;
											}
										}

										//										log.trace("TripleStorageMongoDBIterator: # }");

										r_triples.length = last_r_triples;

										//										log.trace("TripleStorageMongoDBIterator: #9 last_r_triples=%d, _name_key_L1=[%s]", last_r_triples, cast(immutable) _name_key_L1);

										//										if (reif_triples is null)
										//											log.trace("TripleStorageMongoDBIterator: reif_triples is null");																					

										FKeys reifed_composite_key = new FKeys(s_reif_parent_triple,
												p_reif_parent_triple, o_reif_parent_triple);
										reif_triples[reifed_composite_key] = r_triples;

										//										log.trace("TripleStorageMongoDBIterator: #10 reifed_composite_key=%s", reifed_composite_key);

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
							if(trace_msg[1019] == 1)
							{
								string _name_key = fromStringz(bson_iterator_key(&it));
								//								log.trace("TripleStorageMongoDBIterator:def:_name_key:", _name_key);
							}
						}
					break;

				}
			}
		}

		//		log.trace("mongo_cursor_destroy(cursor)");

		mongo_cursor_destroy(cursor);
		cursor = null;

		return 0;
	}

	int length()
	{
		int count = 0;

		//		foreach(tt; this)
		//		{
		//			count++;
		//			log.trace("length:%s", tt);
		//		}

		return count;
	}
}

struct CacheInfo
{
	int count = 0;
	long lifetime;
	bool isCached = false;
}

class TripleStorageMongoDB: TripleStorage
{
	string query_log_filename = "triple-storage-io";

	private long total_count_queries = 0;

	private char[] buff = null;
	private char* col = cast(char*) "coll1";
	private char* ns = cast(char*) "coll1.simple";

	private bool[char[]] predicate_as_multiple;
	private bool[char[]] multilang_predicates;
	private bool[char[]] fulltext_indexed_predicates;

	private bool log_query = false;

	private mongo conn;

	byte caching_strategy;
	TripleStorageMemory ts_in_mem;
	CacheInfo*[hash_t] queryes;
	int count_cached_queryes;

	this(string host, int port, string collection, byte _caching_strategy = caching_type.NONE)
	{
		caching_strategy = _caching_strategy;

		col = cast(char*) collection;
		ns = cast(char*) (collection ~ ".simple");

		int err = mongo_connect(&conn, host, port);
		if(err != MONGO_OK)
		{
			log.trace("failed to connect to mongodb, err=%s", mongo_error_str[conn.err]);
			throw new Exception("failed to connect to mongodb");
		}
		log.trace("connect to mongodb sucessful");
		mongo_set_op_timeout(&conn, 1000);

		init();
	}

	int _tmp_hhh = 0;

	private void init()
	{
		if(caching_strategy == caching_type.ALL_DATA || caching_strategy == caching_type.QUERY_RESULT)
		{
			ts_in_mem = new TripleStorageMemory();

			if(caching_strategy == caching_type.ALL_DATA)
			{
				// для этой стратегии кеширования, следует загрузить весь кеш, данными из mongodb
			}

		}
	}

	public void set_log_query_mode(bool on_off)
	{
		log_query = on_off;

	}

	public void define_predicate_as_multiple(string predicate)
	{
		predicate_as_multiple[predicate] = true;

		if(ts_in_mem !is null)
			ts_in_mem.define_predicate_as_multiple(predicate);

		log.trace("TSMDB:define predicate [%s] as multiple", predicate);
	}

	public void define_predicate_as_multilang(string predicate)
	{
		multilang_predicates[predicate] = true;

		if(ts_in_mem !is null)
			ts_in_mem.define_predicate_as_multilang(predicate);

		log.trace("TSMDB:define predicate [%s] as multilang", predicate);
	}

	public void set_fulltext_indexed_predicates(string predicate)
	{
		fulltext_indexed_predicates[predicate] = true;

		if(ts_in_mem !is null)
			ts_in_mem.set_fulltext_indexed_predicates(predicate);

		log.trace("TSMDB:set fulltext indexed predicate [%s]", predicate);
	}

	public bool f_trace_list_pull = true;

	//	public void set_new_index(ubyte index, uint max_count_element, uint max_length_order, uint inital_triple_area_length)
	//	{
	//	}

	public void set_stat_info_logging(bool flag)
	{
	}

	//	public void setPredicatesToS1PPOO(char[] _P1, char[] _P2, char[] _store_predicate_in_list_on_idx_s1ppoo)
	//	{
	//		P1 = _P1;
	//		P2 = _P2;
	//		store_predicate_in_list_on_idx_s1ppoo = _store_predicate_in_list_on_idx_s1ppoo;
	//	}

	public bool removeSubject(string s)
	{
		try
		{
//			writeln("remove ", s);

			bson cond;

			bson_init(&cond);
			_bson_append_string(&cond, "@", s);

			bson_finish(&cond);
			mongo_remove(&conn, ns, &cond);

			bson_destroy(&cond);

			return true;
		} catch(Exception ex)
		{
			log.trace("ex! removeSubject %s", s);
			return false;
		}
	}

	public bool isExistSubject(string subject)
	{
		//		if(ts_mem !is null)
		//			return ts_mem.isExistSubject(subject);

		StopWatch sw;
		sw.start();

		bool res = false;

		bson query;
		bson fields;
		bson out_data;

		bson_init(&query);
		bson_init(&fields);

		if(subject !is null)
			_bson_append_string(&query, "@", subject);

		bson_finish(&query);
		bson_finish(&fields);

		if(mongo_find_one(&conn, ns, &query, &fields, &out_data) == 0)
			res = true;

		bson_destroy(&fields);
		bson_destroy(&query);
		bson_destroy(&out_data);

		sw.stop();
		long t = cast(long) sw.peek().usecs;

		if(t > 50000)
		{
			log.trace("isExistSubject [%s], total time: %d[µs]", subject, t);
		}
		return res;
	}

	public bool removeTriple(string s, string p, string o)
	{
		//		log.trace("TripleStorageMongoDB:remove triple <" ~ s ~ "><" ~ p ~ ">\"" ~ o ~ "\"");

		if(s is null || p is null || o is null)
		{
			throw new Exception("remove triple:s is null || p is null || o is null");
		}

		bson query;
		bson fields;

		bson_init(&query);
		bson_init(&fields);

		_bson_append_string(&query, "@", s);
		_bson_append_string(&query, p, o);

		bson_finish(&query);
		bson_finish(&fields);

		mongo_cursor* cursor = mongo_find(&conn, ns, &query, &fields, 1, 0, 0);

		if(mongo_cursor_next(cursor))
		{
			bson_iterator it;
			bson_iterator_init(&it, &cursor.current);
			switch(bson_iterator_type(&it))
			{
				case bson_type.BSON_STRING:
					log.trace("remove! string");
				break;

				case bson_type.BSON_ARRAY:
					log.trace("remove! array");
				break;

				default:
				break;
			}

		} else
		{
			throw new Exception(
					"remove triple <" ~ cast(string) s ~ "><" ~ cast(string) p ~ ">\"" ~ cast(string) o ~ "\": triple not found");
		}

		mongo_cursor_destroy(cursor);
		bson_destroy(&fields);
		bson_destroy(&query);

		{
			bson op;
			bson cond;

			bson_init(&cond);
			_bson_append_string(&cond, "@", s);

			//			if(p == HAS_PART)
			//			{
			//				bson_buffer_init(&bb);
			//				bson_buffer* sub = bson_append_start_object(&bb,
			//						"$pull");
			//				bson_append_int(sub, p.ptr, 1);
			//				bson_append_finish_object(sub);
			//			} else
			//			{
			bson_init(&op);
			_bson_append_start_object(&op, "$unset");
			_bson_append_int(&op, p, 1);
			bson_append_finish_object(&op);
			//			}

			bson_finish(&op);
			bson_finish(&cond);

			mongo_update(&conn, ns, &cond, &op, 0);

			bson_destroy(&cond);
			bson_destroy(&op);
		}

		//		if(cache_query_result !is null)
		//			cache_query_result.removeTriple(s, p, o);

		//		if(log_query == true)
		//			logging_query("REMOVE", s, p, o, null);

		return true;
	}

	public void addTripleToReifedData(Triple reif, string p, string o, byte lang)
	{
		//  {SUBJECT:[$reif_subject]}{$set: {'_reif_[$reif_predicate].[$reif_object].[$p]' : [$o]}});
		Triple newtt = new Triple(reif.S, "_reif_" ~ reif.P ~ "." ~ reif.O ~ "." ~ p ~ "", o, lang);

		addTriple(newtt);
	}

	public int addTriple(Triple tt)
	{
		StopWatch sw;
		sw.start();

		bson op;
		bson cond;

		bson_init(&cond);
		_bson_append_string(&cond, "@", tt.S);

		bson_init(&op);

		if((tt.P in predicate_as_multiple) !is null)
		{
			_bson_append_start_object(&op, "$addToSet");

			if(tt.lang == _NONE)
				_bson_append_string(&op, tt.P, tt.O);
			else if(tt.lang == _RU)
				_bson_append_string(&op, tt.P, tt.O ~ "@ru");
			if(tt.lang == _EN)
				_bson_append_string(&op, tt.P, tt.O ~ "@en");

			bson_append_finish_object(&op);
		} else
		{
			if(tt.lang == _NONE)
			{
				_bson_append_start_object(&op, "$set");
				_bson_append_string(&op, tt.P, tt.O);
			} else if(tt.lang == _RU)
			{
				_bson_append_start_object(&op, "$addToSet");
				_bson_append_string(&op, tt.P, tt.O ~ "@ru");
			} else if(tt.lang == _EN)
			{
				_bson_append_start_object(&op, "$addToSet");
				_bson_append_string(&op, tt.P, tt.O ~ "@en");
			}

			bson_append_finish_object(&op);
		}

		bson_finish(&cond);
		bson_finish(&op);

		//		log.trace ("query FT %s", bson_to_string (&op));
		mongo_update(&conn, ns, &cond, &op, 1);

		bson_destroy(&op);
		
		// добавим данные для полнотекстового поиска
		char[][] aaa;

		if((tt.P in fulltext_indexed_predicates) !is null)
		{
			bson_init(&op);
			_bson_append_start_object(&op, "$addToSet");

			//			bson_buffer* sub1 = bson_append_start_object(sub, "_keywords");
			//			bson_buffer* sub2 = bson_append_start_array(sub1, cast(char*) "$each");			

			char[] l_o = cast(char[]) toLower(tt.O);

			if(l_o.length > 2)
			{
				_bson_append_string(&op, "_keywords", cast(string) l_o);

				bson_append_finish_object(&op);

				bson_finish(&op);
				mongo_update(&conn, ns, &cond, &op, 1);

				for(int ic = 0; ic < l_o.length; ic++)
				{
					if((l_o[ic] == '-' && ic == 0) || l_o[ic] == '"' || l_o[ic] == '\'' || (l_o[ic] == '@' && (l_o.length - ic) > 4) || l_o[ic] == '\'' || l_o[ic] == '\\' || l_o[ic] == '.' || l_o[ic] == '+')
						l_o[ic] = ' ';
				}

				aaa = split(l_o, " ");
				
				bson_destroy(&op);

				bson_init(&op);
				_bson_append_start_object(&op, "$addToSet");
				_bson_append_start_object(&op, "_keywords");
				_bson_append_start_array(&op, "$each");

				foreach(aa; aaa)
				{
					if(aa.length > 2)
					{
						_bson_append_string(&op, "", cast(string) aa);
					}
				}

				bson_append_finish_object(&op);
				bson_append_finish_object(&op);
				bson_append_finish_object(&op);

				bson_finish(&op);
				mongo_update(&conn, ns, &cond, &op, 1);
				
				bson_destroy(&op);				
			}
		}

		//		bson_from_buffer(&op, &bb);
		//		log.trace ("query FT %s", bson_to_string (&op));
		//		mongo_update(&conn, ns, &cond, &op, 1);

		//		Thread.getThis().sleep(100_000);

		bson_destroy(&cond);
		bson_destroy(&op);

		sw.stop();
		long t = cast(long) sw.peek().usecs;

		if(t > 300 || trace_msg[1042] == 1)
		{
			log.trace("total time add triple: %d[µs]", t);
		}

		if(ts_in_mem !is null)
		{
			ts_in_mem.addTriple(tt);
		}

		return 0;
	}

	public TLIterator getTriples(string s, string p, string o)
	{
		CacheInfo* ci;

		if(ts_in_mem !is null)
		{
			hash_t hash_of_query = get_hash_of_query(s, p, o);

			if(hash_of_query in queryes)
			{
				ci = queryes[hash_of_query];
				ci.count++;

				if(ci.isCached == true)
					return ts_in_mem.getTriples(s, p, o);

				//				log.trace ("query [<%s><%s><%s>] count requests [%d], count queries = %d", s, p, o, ci.count, queryes.length);
			} else
			{
				CacheInfo* ci = new CacheInfo;
				ci.count = 1;
				queryes[hash_of_query] = ci;
			}
		}

		//		StopWatch sw;
		//		sw.start();

		total_count_queries++;

		bool f_is_query_stored = false;

		bson query;
		bson fields;

		bson_init(&query);
		bson_init(&fields);

		if(s !is null)
		{
			_bson_append_string(&query, "@", s);
		}

		if(p !is null && o !is null)
		{
			_bson_append_string(&query, p, o);
		}

		//			bson_append_int(&bb2, cast(char*)"@", 1);
		//			if (p !is null)
		//			{
		//				bson_append_stringA(&bb2, p, cast(char[]) "1");
		//			}

		bson_finish(&query);
		bson_finish(&fields);

		mongo_cursor* cursor = mongo_find(&conn, ns, &query, &fields, MAX_SIZE_READ_RECORDS, 0, 0);
		if(cursor is null)
		{
			log.trace("ex! getTriples:mongo_find, err=%s", mongo_error_str[conn.err]);
			throw new Exception("getTriples:mongo_find, err=" ~ mongo_error_str[conn.err]);
		}

		TLIterator it;

		if(ts_in_mem !is null && need_caching(ci, s, p, o))
		{
			// такой запрос кешируем
			if(ci.isCached == false)
			{
				it = new TripleStorageMongoDBIterator(cursor, ts_in_mem);

				ci.isCached = true;
				count_cached_queryes++;
				log.trace("caching query [<%s><%s><%s>]", s, p, o);
				log.trace("query count queries = %d, count_cached_queryes=%d", queryes.length, count_cached_queryes);
			} else
			{
				log.trace("mongodb:getTriples:*^&*&%%&$^%#%@");
			}

		} else
		{
			it = new TripleStorageMongoDBIterator(cursor);
		}

		bson_destroy(&fields);
		bson_destroy(&query);

		return it;
	}

	public TLIterator getTriplesOfMask(ref Triple[] mask_triples, byte[char[]] reading_predicates)
	{
		if(mask_triples !is null && mask_triples.length == 2 && mask_triples[0].S !is null && mask_triples[0].P is null && mask_triples[0].O is null && mask_triples[1].S is null && mask_triples[1].P !is null && mask_triples[1].O !is null)
		{
			mask_triples[0].P = mask_triples[1].P; 
			mask_triples[0].O = mask_triples[1].O;
			mask_triples.length = 1;
//			log.trace("!!!");
		}

		CacheInfo* ci;

		if(ts_in_mem !is null)
		{
			hash_t hash_of_query = get_hash_of_query(mask_triples);

			if(hash_of_query in queryes)
			{
				ci = queryes[hash_of_query];
				ci.count++;
				//				log.trace ("caching query [%s] count requests [%d], count queries = %d", mask_triples, ci.count, queryes.length);								 				
			} else
			{
				CacheInfo* ci = new CacheInfo;
				ci.count = 1;
				queryes[hash_of_query] = ci;
			}
		}

		int count_of_reifed_data = 0;

		StopWatch sw;
		sw.start();

		if(trace_msg[1001] == 1)
			log.trace("getTriplesOfMask START mask_triples.length=%d\n", mask_triples.length);

		try
		{
			List list = new List;

			bson query;
			bson fields;

			if(mask_triples.length == 0)
			{
				if(trace_msg[1022] == 1)
					log.trace("getTriplesOfMask:mask_triples.length == 0, return");

				return null;
			}
			
			bson_init(&query);
			bson_init(&fields);

			//			bson_append_stringA(&bb2, cast(char[]) "@", cast(char[]) "1");

			for(short i = 0; i < mask_triples.length; i++)
			{
				string s = mask_triples[i].S;
				string p = mask_triples[i].P;
				string o = mask_triples[i].O;

				if(trace_msg[1002] == 1)
				{
					log.trace("getTriplesOfMask i=%d <%s><%s><%s>", i, s, p, o);
					if (o is null)
					    log.trace ("o is null");
				}

				if(s !is null && s.length > 0)
				{
					add_to_query("@", s, &query);
				}

				if(p !is null && p == "query:fulltext")
				{
					add_fulltext_to_query(o, &query);
				} else if(p !is null /*&& o !is null && o.length > 0*/)
				{
					add_to_query(p, o, &query);
				}
			}

			reading_predicates["@"] = field.GET;

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

			bson_finish(&query);
			bson_finish(&fields);

			if(trace_msg[1005] == 1)
			{
				char[] ss = bson_to_string(&query);
				log.trace("getTriplesOfMask:QUERY:\n %s", ss);
				//				log.trace("---- readed fields=%s", reading_predicates);
			}

			StopWatch sw0;
			sw0.start();

			mongo_cursor* cursor;

			cursor = mongo_find(&conn, ns, &query, &fields, MAX_SIZE_READ_RECORDS, 0, 0);
			if(cursor is null)
			{
				log.trace("ex! getTriplesOfMask:mongo_find, err=%s", mongo_error_str[conn.err]);
				throw new Exception("getTriplesOfMask:mongo_find, err=" ~ mongo_error_str[conn.err]);
			}

			sw0.stop();

			long t0 = cast(long) sw0.peek().usecs;

			if(t0 > 5000)
			{
				char[] ss = bson_to_string(&query);
				log.trace("getTriplesOfMask: QUERY:\n %s", ss);
				log.trace("getTriplesOfMask: mongo_find: %d[µs]", t0);
			}

			TLIterator it;

			if(ts_in_mem !is null && need_caching(ci, mask_triples))
			{
				// такой запрос кешируем
				if(ci.isCached == false)
				{
					it = new TripleStorageMongoDBIterator(cursor, reading_predicates, ts_in_mem);

					ci.isCached = true;
					count_cached_queryes++;
					log.trace("caching query [%s]", mask_triples);
					log.trace("query count queries = %d, count_cached_queryes=%d", queryes.length, count_cached_queryes);
				} else
				{
					log.trace("mongodb:getMaskTriples:*^&*&%%&$^%#%@");
				}

			} else
			{
				it = new TripleStorageMongoDBIterator(cursor, reading_predicates);
			}

			bson_destroy(&fields);
			bson_destroy(&query);

			return it;
		} catch(Exception ex)
		{
			log.trace("@exception:%s", ex.msg);
			throw ex;
		}

	}

	private void add_to_query(string field_name, string field_value, bson* bb)
	{
		if(trace_msg[1030] == 1)
			log.trace("add_to_query ^^^ field_name = %s, field_value=%s", field_name, field_value);

		bool field_is_multilang = (field_name in multilang_predicates) !is null;

		if(field_value !is null && (field_value[0] == '"' && field_value[1] == '[' || field_value[0] == '['))
		{
			if(field_value[0] == '[')
				field_value = field_value[1 .. field_value.length - 1];
			else
				field_value = field_value[2 .. field_value.length - 2];

			string[] values = split(field_value, ",");
			if(values.length > 0)
			{
				_bson_append_start_array(bb, "$or");
				foreach(val; values)
				{
					_bson_append_start_object(bb, "");

					if(field_is_multilang)
						_bson_append_string(bb, field_name, val ~ "@ru");
					else
						_bson_append_string(bb, field_name, val);

					bson_append_finish_object(bb);
				}
				bson_append_finish_object(bb);
			}
		} else
		{
			if(field_is_multilang)
				_bson_append_string(bb, field_name, field_value ~ "@ru");
			else
				_bson_append_string(bb, field_name, field_value);
		}

		if(trace_msg[1031] == 1)
			log.trace("add_to_query return");
	}

	//
	public void print_stat()
	{
		//		log.trace("TripleStorage:stat: max used pull={}, max length list={}", max_use_pull, max_length_list);
	}

}

char[] getString(char* s)
{
	return s ? s[0 .. strlen(s)] : null;
}

char[] bson_to_string(bson* b)
{
	OutBuffer outbuff = new OutBuffer();
	bson_raw_to_string(b, 0, outbuff);
	outbuff.write(0);
	return getString(cast(char*) outbuff.toBytes());
}

void bson_raw_to_string(bson* b, int depth, OutBuffer outbuff, bson_iterator* ii = null)
{
	bson_iterator* i;
	char* key;
	int temp;
	char oidhex[25];

	if(ii is null)
	{
		i = new bson_iterator;
		bson_iterator_init(i, b);
	} else
		i = ii;

	while(bson_iterator_next(i))
	{
		bson_type t = bson_iterator_type(i);
		if(t == 0)
			break;

		key = bson_iterator_key(i);

		for(temp = 0; temp <= depth; temp++)
			outbuff.write(cast(char[]) "\t");

		outbuff.write(getString(key));
		outbuff.write(cast(char[]) ":");

		switch(t)
		{
			case bson_type.BSON_INT:
				outbuff.write(cast(char[]) "int ");
				outbuff.write(bson_iterator_int(i));
			break;

			case bson_type.BSON_DOUBLE:
				outbuff.write(cast(char[]) "double ");
				outbuff.write(bson_iterator_double(i));
			break;

			case bson_type.BSON_BOOL:
				outbuff.write(cast(char[]) "bool ");
				outbuff.write((bson_iterator_bool(i) ? cast(char[]) "true" : cast(char[]) "false"));
			break;

			case bson_type.BSON_STRING:
				outbuff.write(cast(char[]) "string ");
				outbuff.write(getString(bson_iterator_string(i)));
			break;

			case bson_type.BSON_REGEX:
				outbuff.write(cast(char[]) "regex ");
				outbuff.write(getString(bson_iterator_regex(i)));
			break;

			case bson_type.BSON_NULL:
				outbuff.write(cast(char[]) "null");
			break;

			//			case bson_type.bson_oid:
			//				bson_oid_to_string(bson_iterator_oid(&i), cast(char*) &oidhex);
			//				printf("%s", oidhex);
			//			break; //@@@ cast (char*)&oidhex)
			case bson_type.BSON_OBJECT:
				outbuff.write(cast(char[]) "\n{");

				bson_iterator i1;
				bson_iterator_subiterator(i, &i1);
				bson_raw_to_string(null, depth + 1, outbuff, &i1);
				outbuff.write(cast(char[]) "\n}");
			break;

			case bson_type.BSON_ARRAY:
				outbuff.write(cast(char[]) "\n[");
				bson_iterator i1;
				bson_iterator_subiterator(i, &i1);
				bson_raw_to_string(null, depth + 1, outbuff, &i1);
				outbuff.write(cast(char[]) "\n]");
			break;
			//			default:
			//				fprintf(stderr, "can't print type : %d\n", t);
		}
		outbuff.write(cast(char[]) "\n");
	}
}

string fromStringz(char* s)
{
	char[] res = s ? s[0 .. strlen(s)] : null;
	return cast(string) res;
}

private void add_fulltext_to_query(string fulltext_param, bson* bb)
{
	_bson_append_start_object(bb, "_keywords");
	_bson_append_start_array(bb, "$all");

	string[] values = split(fulltext_param, ",");
	foreach(val; values)
	{
		_bson_append_regex(bb, " ", val, "imx");
	}

	bson_append_finish_object(bb);
	bson_append_finish_object(bb);
}

private hash_t get_hash_of_query(string S, string P, string O)
{
	hash_t res = 0;

	if(S !is null)
		res = hashOf(S.ptr, S.length, 0);

	if(P !is null)
		res += hashOf(P.ptr, P.length, 0);

	if(O !is null)
		res += hashOf(O.ptr, O.length, 0);

	return res;
}

private hash_t get_hash_of_query(ref Triple[] mask_triples)
{
	hash_t res = 0;

	for(short i = 0; i < mask_triples.length; i++)
	{
		res += mask_triples[i].toHash();
	}

	return res;
}

private bool need_caching(CacheInfo* ci, string s, string p, string O)
{
//	log.trace("need_caching ? query [<%s><%s><%s>] %d", s, p, O, ci !is null ? ci.count : -1);
	if(ci !is null && ci.isCached == false && ci.count > 100)
	{
		if(O !is null && O[0] == '^')
			return false;

//		log.trace("yes");
		return true;
	}
	return false;
}

private bool need_caching(CacheInfo* ci, ref Triple[] mask_triples)
{
//	log.trace("need_caching ? query [%s] %d ci=%x", mask_triples, ci !is null ? ci.count : -1, ci);
	if(ci !is null && ci.isCached == false && ci.count > 100 && mask_triples.length == 1)
	{

		for(short i = 0; i < mask_triples.length; i++)
		{
			if(mask_triples[i].O !is null && mask_triples[i].O[0] == '^')
				return false;
		}

//		log.trace("yes");
		return true;
	}
	return false;
}
