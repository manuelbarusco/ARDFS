# ARDFS 
ARDFS (Acordar RDF Search) repository contains the Java code for the indexing and searching phases over the  [ACORDAR](https://dome40.eu/sites/default/files/2022-11/ACORDAR%20A%20Test%20Collection%20for%20Ad%20Hoc%20Content-Based%20%28RDF%29%20Dataset%20Retrieval.pdf) dataset collection. These are the final steps of a more complete reproducibility study based on the [ACORDAR Paper](https://dome40.eu/sites/default/files/2022-11/ACORDAR%20A%20Test%20Collection%20for%20Ad%20Hoc%20Content-Based%20%28RDF%29%20Dataset%20Retrieval.pdf) proposed systems.  

The indexing phase code is contatined in the <code>/index</code> directory and is performed by the <code>DatasetIndexer.java</code> tool or its derived versions. 
The searching phase code is contatined in the <code>/search</code> directory and is performed by the <code>DatasetSearcher.java</code> tool. The FSDM ranking is implemented by the <code>FSDMRanker.java</code>

All the implementation details are decided based on the ACORDAR paper and [repository](https://github.com/nju-websoft/ACORDAR-2) details.

