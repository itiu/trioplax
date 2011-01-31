module trioplax.TripleStorage;

private import trioplax.triple;

public immutable byte _NONE = 0;
public immutable byte _RU = 1;
public immutable byte _EN = 2;

public immutable byte _GET = 0;
public immutable byte _GET_REIFED = 1;

interface TripleStorage
{
	// main functions	
	public int addTriple(string s, string p, string o, byte lang=_NONE);
	public void addTripleToReifedData(string reif_subject, string reif_predicate, string reif_object, string p, string o, byte lang = _NONE);
	
	public List getTriples(string s, string p, string o);
	public List getTriplesOfMask(ref Triple[] triples, byte[char[]] read_predicates);
	
	public bool isExistSubject (string subject); 
	
	public bool removeTriple(char[] s, char[] p, char[] o);
	
	// configure functions	
	public void set_new_index(ubyte index, uint max_count_element, uint max_length_order, uint inital_triple_area_length);
	
	public void define_predicate_as_multiple(char[] predicate);	
	public void setPredicatesToS1PPOO(char[] P1, char[] P2, char[] _store_predicate_in_list_on_idx_s1ppoo);

	public void set_stat_info_logging(bool flag);		
	public void set_log_query_mode (bool on_off);	
	////////////////////////////////////////
		
	public void print_stat();

	////////////////////////////////////////	
//	private void logging_query(char[] op, Triple [] mask, List list);	
}
