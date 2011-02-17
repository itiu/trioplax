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
	bool[TwoKeys] POPO_list;
	string[string] P1P2_for_POPO;
	string[string] P2P1_for_POPO;

	List[FourKeys] iPOPO;
	List[ThreeKeys] iSPO;
	List[TwoKeys] iSP;
	List[TwoKeys] iPO;
	List[TwoKeys] iSO;
	List[string] iS;
	List[string] iP;
	List[string] iO;

	public int addTriple(Triple tt)
	{
		//		log.trace("add triple into mem: [%s] [%s] [%s]", tt.S, tt.P, tt.O);

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
		//		log.trace("#getTriples [%s] [%s] [%s]", _S, _P, _O);

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

	public List getTriplesOfMask(ref Triple[] triples, byte[char[]] reading_predicates)
	{
		List res = null;

		// это двойной ключ PPOO ?
		if(triples.length == 2)
		{
			log.trace("getTriplesOfMask > %s", triples);
			log.trace("current POPO_list = %s", POPO_list);

			// да
			TwoKeys pp = new TwoKeys(triples[0].P, triples[1].P);
			//	 проверить есть ли в списке заиндексированных двойных ключей
			if((pp in POPO_list) is null)
			{
				log.trace("построим индекс для [%s][%s]", triples[0].P, triples[1].P);

				// нет, сохранить в списке этот ключ
				POPO_list[pp] = true;
				// сохранить в списках позволяющих понять порядк P1P2 в индексе
				P1P2_for_POPO[triples[0].P] = triples[1].P;
				P2P1_for_POPO[triples[1].P] = triples[0].P;
				// и произвести построение по нему индекса

				//	перебор всех фактов из индекса iSPO и добавление требуемых в индекс PPOO
				foreach(ol; iSPO.values)
				{
					Triple tt = ol.lst.data[0];

					string P1;
					string O1;
					string P2;
					string O2;

					string* _P2;

					// при добавлении нужно понять какой из P добавляется, P1 или P2
					// нужно проверить есть ли недостающий факт для данного субьекта с предикатом PX
					// если есть то можно добавлять в индекс 
					// если такого факта не нашлось, то пропускаем добавление в iPPOO;													
					if(_P2 !is null)
					{
						P2 = *_P2;
						P1 = tt.P;
						O1 = tt.O;

						// нужно найти O2
						List res1 = getTriples(tt.S, P2, null);

						if(res1 !is null)
							O2 = res1.lst.data[0].O;
					}
					else
					{
						string* _P1 = (tt.P in P1P2_for_POPO);

						if(_P1 !is null)
						{
							P1 = *_P1;
							P2 = tt.P;
							O2 = tt.O;

							// нужно найти O1
							List res1 = getTriples(tt.S, P1, null);

							if(res1 !is null)
								O1 = res1.lst.data[0].O;

						}
					}

					if(P1 !is null && P2 !is null && O1 !is null && O2 !is null)
					{
						// можно добавлять в индекс
						// TODO исключить добавление в список одинаковых фактов
						addIntoFourIndex(iPOPO, P1, O1, P2, O2, tt);
					}

				}

				log.trace("in iPOPO: \n %s", iPOPO);
			}
			//   произвести поиск по этому индексу   
			// ! нужно учитывать порядок P1P2
			FourKeys key = new FourKeys(triples[0].P, triples[0].O, triples[1].P, triples[1].O);

			res = iPOPO.get(key, res);
		}

		res = getTriples(triples[0].S, triples[0].P, triples[0].O);
		List outl = new List;

		// цикл по найденным субьектам
		if(res !is null)
		{
			//			log.trace("found=%s", res.lst.data);
			//			log.trace("reading_predicates=%s", reading_predicates);

			foreach(el; res.lst.data)
			{
				foreach(pp; reading_predicates.keys)
				{
					//					log.trace("el=[%s], pp=%s", el, pp);

					List res1 = getTriples(el.S, cast(immutable) pp, null);

					if(res1 !is null)
					{
						foreach(el1; res1.lst.data)
						{
							//							log.trace("el1=%s", el1);
							outl.lst.put(el1);
						}
					}

				}

			}
		}

		log.trace("ok");

		return outl;
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

	private void addIntoFourIndex(ref List[FourKeys] idx, string _key1, string _key2, string _key3, string _key4, Triple tt)
	{
		FourKeys xx = new FourKeys(_key1, _key2, _key3, _key4);

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

		char[] key = new char[_key1.length];
		key[0 .. $] = _key1[0 .. $];

		//		log.trace ("add into mem: key=%s", key);

		apnpdr = idx.get(cast(immutable) key, apnpdr);
		if(apnpdr is null)
		{
			apnpdr = new List;
			idx[cast(immutable) key] = apnpdr;
		}
		apnpdr.lst.put(tt);
	}
}
