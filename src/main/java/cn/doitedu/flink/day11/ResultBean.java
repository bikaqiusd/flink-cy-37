package cn.doitedu.flink.day11;

public class ResultBean {

    public String cid;
    public String eid;
    public String pid;
    public long counts;
    public int ord;
    public long winStart;
    public long winEnd;



    public ResultBean(String cid, String eid, String pid, long counts, long winStart, long winEnd) {
        this.cid = cid;
        this.eid = eid;
        this.pid = pid;
        this.counts = counts;
        this.winStart = winStart;
        this.winEnd = winEnd;
    }


    public ResultBean() {
    }
}
