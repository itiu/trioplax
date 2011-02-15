module trioplax.memory.TripleStorageMemory;

private import trioplax.TripleStorage;
private import trioplax.memory.ComplexKeys;
private import trioplax.triple;

private import std.stdio;

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
			//                      writeln("triple ", t, " already exist in index");
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
		List apnpdr;

		if(_S !is null && _P !is null && _O !is null)
		{
			ThreeKeys spo = new ThreeKeys(_S, _P, _O);

			apnpdr = iSPO.get(spo, apnpdr);
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
			apnpdr = iS.get(_S, apnpdr);
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
		apnpdr = idx.get(_key1, apnpdr);
		if(apnpdr is null)
		{
			apnpdr = new List;
			idx[_key1] = apnpdr;
		}
		apnpdr.lst.put(tt);

		//          if (_key1 == "<http://www.census.gov/tiger/2002/vocab#start>")
		{
			//                  List apnpdr1;
			//                  apnpdr1 = idx.get("<http://www.census.gov/tiger/2002/vocab#start>", apnpdr1);

			//          if(apnpdr !is null && apnpdr.lst.data.length > 85500)
			//          {
			//                  writeln("@2 get iP.ptr", cast(void*)&iP);
			//                  writeln("apnpdr.ptr =", cast(void*)apnpdr);
			//                  writeln("key1 = [", _key1, "]");
			//                  writeln("apnpdr.data.length =", apnpdr.lst.data.length);
			//                  writeln("iP,keys =", iP.keys);
			//          }
		}

	}
}
