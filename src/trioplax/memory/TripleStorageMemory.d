// TODO array->iterator
module trioplax.memory.TripleStorageMemory;

private import std.stdio;
private import std.datetime;

private import trioplax.TripleStorage;
private import trioplax.memory.ComplexKeys;
private import trioplax.triple;
private import trioplax.Logger;
private import std.format;

Logger log;

static this()
{
	log = new Logger("trioplax", "log", "");
}

class List
{
	bool[Triple] lst;

	Triple[] array()
	{
		return lst.keys();
	}

	void put(Triple tt)
	{
		lst[tt] = true;
	}

	override string toString()
	{
		auto writer = appender!string();

		formattedWrite(writer, "[%d]%s \n", array.length, array);

		return writer.data;
	}
}

class TripleStorageMemoryIterator: TLIterator
{
	List list;
	int current_position = 0;

	this(List _list)
	{
		list = _list;
	}

	int opApply(int delegate(ref Triple) dg)
	{
		int result = 0;

		for(int i = 0; i < list.lst.keys.length; i++)
		{
			result = dg(list.lst.keys[i]);
			if(result)
				break;
		}
		return result;
	}

    int length ()
    {
    	return 0;
    }
	
}

class TripleStorageMemory: TripleStorage
{
	bool[FKeys] POPO_list;

	List[FKeys] iSPO;
	List[FKeys] iSP;
	List[FKeys] iPO;
	List[FKeys] iSO;
	List[FKeys] iS;
	List[FKeys] iP;
	List[FKeys] iO;
	
	private bool[char[]] predicate_as_multiple;
	private bool[char[]] multilang_predicates;
	private bool[char[]] fulltext_indexed_predicates;	

	void _tmp_print_iSPO()
	{
		foreach(akey; iSPO.keys)
		{
			log.trace("#iSPO.key = %s", akey);

			List ll = iSPO[akey];
			foreach(tt; ll.array)
			{
				log.trace("# tt=%s", tt);
			}
		}

	}

	void _tmp_print_iSP()
	{
		foreach(akey; iSP.keys)
		{
			log.trace("#iSP.key = %s", akey);

			List ll = iSP[akey];
			foreach(tt; ll.array)
			{
				log.trace("# tt=%s", tt);
			}
		}
	}

	public int addTriple(Triple tt)
	{
		if (tt.S is null || tt.P is null || tt.O is null)
			return 0;
		
		FKeys spo = new FKeys(tt.S, tt.P, tt.O);

		List apnpdr;
		apnpdr = iSPO.get(spo, apnpdr);

		if(apnpdr is null)
		{
			apnpdr = new List;
			iSPO[spo] = apnpdr;
			apnpdr.put(tt);
		}
		else
		{
//			log.trace("triple %s already exist in index", tt);
			return -1;
		}

		addIntoIndex(iSP, tt, tt.S, tt.P);
		addIntoIndex(iPO, tt, tt.P, tt.O);
		addIntoIndex(iSO, tt, tt.S, tt.O);
		addIntoIndex(iS, tt, tt.S);
		addIntoIndex(iP, tt, tt.P);
		addIntoIndex(iO, tt, tt.O);

		log.trace("add triple into mem: %s, spo.length=%d", tt, iSPO.length);
		
		return 1;
	}

	public void addTripleToReifedData(Triple reif, string p, string o, byte lang)
	{
	}

	public TLIterator getTriples(string _S, string _P, string _O)
	{
		//		StopWatch sw;
		//		sw.start();

		log.trace("getTriples from mem [%s] [%s] [%s]", _S, _P, _O);

		List apnpdr;

		if(_S !is null && _P !is null && _O !is null)
		{
			//			log.trace ("#getTriples iSPO");
			FKeys spo = new FKeys(_S, _P, _O);

			apnpdr = iSPO.get(spo, apnpdr);
			//			log.trace ("#getTriples iSPO ok");
			//			log.trace ("#getTriples apnpdr=%s", apnpdr);
			//			log.trace ("#getTriples apnpdr.lst=%s", apnpdr.lst);
			//			log.trace ("#getTriples apnpdr.lst.data=%s", apnpdr.lst.data);
			//			log.trace ("#getTriples apnpdr.lst.data.length=%d", apnpdr.lst.data.length);
		}
		else if(_S !is null && _P !is null && _O is null)
		{
			FKeys sp = new FKeys(_S, _P);

			apnpdr = iSP.get(sp, apnpdr);
		}
		else if(_S is null && _P !is null && _O !is null)
		{
			FKeys po = new FKeys(_P, _O);

			apnpdr = iPO.get(po, apnpdr);
		}
		else if(_S !is null && _P is null && _O !is null)
		{
			FKeys so = new FKeys(_S, _O);

			apnpdr = iSO.get(so, apnpdr);
		}
		else if(_S !is null && _P is null && _O is null)
		{
			//			log.trace ("#getTriples iS");
			FKeys s = new FKeys(_S);
			apnpdr = iS.get(s, apnpdr);
			//			log.trace ("#getTriples iS ok");
			//			log.trace ("#getTriples apnpdr=%s", apnpdr);
			//			log.trace ("#getTriples apnpdr.lst=%s", apnpdr.lst);
			//			log.trace ("#getTriples apnpdr.lst.data=%s", apnpdr.lst.data);
			//			log.trace ("#getTriples apnpdr.lst.data.length=%d", apnpdr.lst.data.length);
		}
		else if(_S is null && _P !is null && _O is null)
		{
			FKeys p = new FKeys(_P);
			apnpdr = iP.get(p, apnpdr);
		}
		else if(_S is null && _P is null && _O !is null)
		{
			FKeys o = new FKeys(_O);
			apnpdr = iO.get(o, apnpdr);
		}

		//		sw.stop();
		//		long t = cast(long) sw.peek().usecs;

		//		if(t > 0)
		//		{
		//			log.trace("memory get triple: %s %s %s %d[µs]", _S, _P, _O, t);
		//		}

		if(apnpdr !is null)
			return new  TripleStorageMemoryIterator (apnpdr);
		else
			return null;

	}

	public TLIterator getTriplesOfMask(ref Triple[] triples, byte[char[]] reading_predicates)
	{
		StopWatch sw;
		sw.start();

		List res = null;

		List outl = new List;

		// цикл по найденным субьектам
		if(res !is null)
		{
			//			log.trace("found=%s", res.lst.data);
			//			log.trace("reading_predicates=%s", reading_predicates);

			sw.stop();
			foreach(el; res.array)
			{
				foreach(pp; reading_predicates.keys)
				{
					//					log.trace("el=[%s], pp=%s", el, pp);

//@@@					List res1 = getTriples(el.S, cast(immutable) pp, null);
					//					log.trace("res1=%s", res1);
//@@@					if(res1 !is null)
//@@@					{
//@@@						foreach(el1; res1.array)
//@@@						{
//@@@							outl.put(el1);
//@@@						}
//@@@					}

				}

			}
			sw.start();
		}

		//		log.trace("ok");

		sw.stop();
		long t = cast(long) sw.peek().usecs;

		if(t > 0)
		{
			log.trace("memory get triples of mask: %s %d[µs]", triples, t);
		}

		return null;
	}

	public bool isExistSubject(string subject)
	{
		List apnpdr;

		FKeys key = new FKeys (subject);
				
		apnpdr = iS.get(key, apnpdr);

		if(apnpdr !is null && apnpdr.array.length > 0)
			return true;

		return false;
	}

	public bool removeTriple(string s, string p, string o)
	{
		return false;
	}

	// configure functions	
//	public void set_new_index(ubyte index, uint max_count_element, uint max_length_order, uint inital_triple_area_length)
//	{
//	}

	public void define_predicate_as_multiple(string predicate)
	{
		predicate_as_multiple[predicate] = true;

		log.trace("TSM:define predicate [%s] as multiple", predicate);
	}

	public void define_predicate_as_multilang(string predicate)
	{
		multilang_predicates[predicate] = true;

		log.trace("TSM:define predicate [%s] as multilang", predicate);
	}

	public void set_fulltext_indexed_predicates(string predicate)
	{
		fulltext_indexed_predicates[predicate] = true;

		log.trace("TSM:set fulltext indexed predicate [%s]", predicate);
	}

//	public void setPredicatesToS1PPOO(char[] P1, char[] P2, char[] _store_predicate_in_list_on_idx_s1ppoo)
//	{
//	}

	public void set_stat_info_logging(bool flag)
	{
	}

	public void set_log_query_mode(bool on_off)
	{
	}

	public void print_stat()
	{
		writeln("iSPO.length=", iSPO.keys.length);
		writeln("iSP.length=", iSP.keys.length);
		writeln("iPO.length=", iPO.keys.length);
		writeln("iSO.length=", iSO.keys.length);
		writeln("iS.length=", iS.keys.length);
		writeln("iP.length=", iP.keys.length);
		writeln("iO.length=", iO.keys.length);
	}

	private void addIntoIndex(ref List[FKeys] idx, Triple tt, string _key1, string _key2 = null, string _key3 = null, string _key4 = null)
	{
		//		writeln("addIntoFourIndex=", tt);
		FKeys xx = new FKeys(_key1, _key2, _key3, _key4);

		List apnpdr;
		apnpdr = idx.get(xx, apnpdr);
		if(apnpdr is null)
		{
			//                      writeln ("###");
			apnpdr = new List;
			idx[xx] = apnpdr;
			//			writeln("iPOPO.length=", idx.keys.length);
		}
		apnpdr.put(tt);
	}
	
	public bool removeSubject(string s)
	{
		return false;
	}
}
