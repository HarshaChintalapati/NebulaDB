package dev.nebuladb.storage;

import java.util.BitSet;

public final class BloomFilter {
    private final BitSet bits; private final int size; private final int hashes;
    public BloomFilter(int size, int hashes) { this.size=Math.max(64,size); this.hashes=Math.max(2,hashes); this.bits=new BitSet(this.size); }
    public void add(String s) { for(int i=0;i<hashes;i++) bits.set(index(s,i)); }
    public boolean mightContain(String s) { for(int i=0;i<hashes;i++) if(!bits.get(index(s,i))) return false; return true; }
    private int index(String s,int seed){ int h=0x811c9dc5 ^ (seed*0x9e3779b9); for(byte b:s.getBytes()){ h^=b; h*=0x01000193; } return Math.floorMod(h,size); }
}
