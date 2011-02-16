module trioplax.memory.TripleStorageMemory;

private import std.stdio;

private import trioplax.TripleStorage;
private import trioplax.memory.ComplexKeys;
private import trioplax.triple;
private import trioplax.Logger;

Logger log;

static this()
{
	log = new Logger("trioplax.log", "");
}

class TripleStorageMemory: TripleStorage
{
	List[ThreeKeys] iSPO;
	List[TwoKeys] iSP;
	List[TwoKeys] iPO;
	List[TwoKeys] iSO;
	List[string] iS;
	List[string] iP;
	List[string] iO;

	public int addTriple(Triple tt)
	{
		log.trace ("add triple into mem: [%s] [%s] [%s]", tt.S, tt.P, tt.O);

		ThreeKeys spo = new ThreeKeys(tt.S, tt.P, tt.O);

		List apnpdr;
		apnpdr = iSPO.get(spo, apnpdr);

		if(apnpdr is null)
		{
			apnpdr = new List;
			iSPO[spo] = apnpdr;
			apnpdr.lst.put(tt);
		}
		else
		{
			log.trace("triple %s already exist in index", tt);
			return -1;
		}

		addIntoTwoIndex(iSP, tt.S, tt.P, tt);
		addIntoTwoIndex(iPO, tt.P, tt.O, tt);
		addIntoTwoIndex(iSO, tt.S, tt.O, tt);
		addIntoOneIndex(iS, tt.S, tt);
		addIntoOneIndex(iP, tt.P, tt);
		addIntoOneIndex(iO, tt.O, tt);

		return 1;
	}

	public void addTripleToReifedData(Triple reif, string p, string o, byte lang)
	{
	}

	public List getTriples(string _S, string _P, string _O)
	{
		log.trace ("#getTriples [%s] [%s] [%s]", _S, _P, _O);
		
		List apnpdr;

		if(_S !is null && _P !is null && _O !is null)
		{
//			log.trace ("#getTriples iSPO");
			ThreeKeys spo = new ThreeKeys(_S, _P, _O);

			apnpdr = iSPO.get(spo, apnpdr);
//			log.trace ("#getTriples iSPO ok");
//			log.trace ("#getTriples apnpdr=%s", apnpdr);
//			log.trace ("#getTriples apnpdr.lst=%s", apnpdr.lst);
//			log.trace ("#getTriples apnpdr.lst.data=%s", apnpdr.lst.data);
//			log.trace ("#getTriples apnpdr.lst.data.length=%d", apnpdr.lst.data.length);
		}
		else if(_S !is null && _P !is null && _O is null)
		{
			TwoKeys sp = new TwoKeys(_S, _P);

			apnpdr = iSP.get(sp, apnpdr);
		}
		else if(_S is null && _P !is null && _O !is null)
		{
			TwoKeys po = new TwoKeys(_P, _O);

			apnpdr = iPO.get(po, apnpdr);
		}
		else if(_S !is null && _P is null && _O !is null)
		{
			TwoKeys so = new TwoKeys(_S, _O);

			apnpdr = iSO.get(so, apnpdr);
		}
		else if(_S !is null && _P is null && _O is null)
		{
//			log.trace ("#getTriples iS");
			apnpdr = iS.get(_S, apnpdr);
//			log.trace ("#getTriples iS ok");
//			log.trace ("#getTriples apnpdr=%s", apnpdr);
//			log.trace ("#getTriples apnpdr.lst=%s", apnpdr.lst);
//			log.trace ("#getTriples apnpdr.lst.data=%s", apnpdr.lst.data);
//			log.trace ("#getTriples apnpdr.lst.data.length=%d", apnpdr.lst.data.length);
		}
		else if(_S is null && _P !is null && _O is null)
		{
			apnpdr = iP.get(_P, apnpdr);
		}
		else if(_S is null && _P is null && _O !is null)
		{
			apnpdr = iO.get(_O, apnpdr);
		}

		if(apnpdr !is null)
			return apnpdr;
		else
			return null;

	}

	public List getTriplesOfMask(ref Triple[] triples, byte[char[]] read_predicates)
	{
		return null;
	}

	public bool isExistSubject(string subject)
	{
		List apnpdr;

		apnpdr = iS.get(subject, apnpdr);

		if(apnpdr !is null && apnpdr.lst.data.length > 0)
			return true;

		return false;
	}

	public bool removeTriple(char[] s, char[] p, char[] o)
	{
		return false;
	}

	// configure functions	
	public void set_new_index(ubyte index, uint max_count_element, uint max_length_order, uint inital_triple_area_length)
	{
	}

	public void define_predicate_as_multiple(char[] predicate)
	{
	}

	public void setPredicatesToS1PPOO(char[] P1, char[] P2, char[] _store_predicate_in_list_on_idx_s1ppoo)
	{
	}

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

	private void addIntoTwoIndex(ref List[TwoKeys] idx, string _key1, string _key2, Triple tt)
	{
		TwoKeys xx = new TwoKeys(_key1, _key2);

		List apnpdr;
		apnpdr = idx.get(xx, apnpdr);
		if(apnpdr is null)
		{
			//                      writeln ("###");
			apnpdr = new List;
			idx[xx] = apnpdr;
			//                      writeln ("iXX.length=", idx.keys.length);
		}
		apnpdr.lst.put(tt);

	}

	private void addIntoOneIndex(ref List[string] idx, string _key1, Triple tt)
	{
		List apnpdr;
		
		char[] key = new char [_key1.length];
		key[0..$] = _key1[0..$];
		
//		log.trace ("add into mem: key=%s", key);
		
		apnpdr = idx.get(cast(immutable)key, apnpdr);
		if(apnpdr is null)
		{
			apnpdr = new List;
			idx[cast(immutable)key] = apnpdr;
		}
		apnpdr.lst.put(tt);
	}
}
