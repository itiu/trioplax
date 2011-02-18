module trioplax.Logger;

private import std.format;
private import std.c.stdio;

version(dmd2_052)
{
    private import std.datetime;
}
else
{
    private import std.datetime;
    private import std.date;
}
//private import std.date;

import std.array: appender;

private import std.stdio;
private import std.datetime;
import std.c.linux.linux;

//package
//{
//	Logger log;
//}

public class Logger
{
	private string trace_logfilename = "app.log";
	private FILE* ff;
	private string src = "";

	//	static this()
	//	{
	//		log = new Logger();
	//	}

	//	static ~this()
	//	{
	//		delete log;
	//	}

	this(string log_name, string _src)
	{
		trace_logfilename = log_name;
		ff = fopen(trace_logfilename.ptr, "aw");
		src = _src;
	}

	~this()
	{
		fclose(ff);;
	}

	void trace_io(bool io, byte* data, int len)
	{
		d_time now = getUTCtime();

		string str_io;

		if(io == true)
			str_io = "INPUT";
		else
			str_io = "OUTPUT";

		int tt = time(null);
		tm* ptm = localtime(&tt);
		int year = ptm.tm_year + 1900;
		int month = ptm.tm_mon;
		int day = ptm.tm_mday;
		int hour = ptm.tm_hour;
		int minute = ptm.tm_min;
		int second = ptm.tm_sec;
		int milliseconds = msFromTime(now);

		auto writer = appender!string();

		formattedWrite(writer, "[%04d-%02d-%02d %02d:%02d:%02d.%03d]\n%s\n", year, month, day, hour, minute, second, milliseconds, str_io);

		writer.put(cast(char) 0);

		fputs(cast(char*) writer.data, ff);

		for(int i = 0; i < len - 1; i++)
		{
			if(*data != 0)
				fputc(*data, ff);
			data++;
		}
		fputc('\r', ff);

		fflush(ff);
	}

	string trace(Char, A...)(in Char[] fmt, A args)
	{
		d_time now = getUTCtime();

		int tt = time(null);
		tm* ptm = localtime(&tt);
		int year = ptm.tm_year + 1900;
		int month = ptm.tm_mon;
		int day = ptm.tm_mday;
		int hour = ptm.tm_hour;
		int minute = ptm.tm_min;
		int second = ptm.tm_sec;
		int milliseconds = msFromTime(now);

		//	       StopWatch sw1; sw1.start();
		auto writer = appender!string();

		formattedWrite(writer, "[%04d-%02d-%02d %02d:%02d:%02d.%03d] [%s] ", year, month, day, hour, minute, second, milliseconds, src);

		formattedWrite(writer, fmt, args);
		writer.put(cast(char) 0);

		fputs(cast(char*) writer.data, ff);
		fputc('\n', ff);

		//    		sw1.stop();
		//               writeln (cast(long) sw1.peek().microseconds);

		fflush(ff);

		return writer.data;
	}

	void trace_log_and_console(Char, A...)(in Char[] fmt, A args)
	{
		write(trace(fmt, args), "\n");
	}
}
