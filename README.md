# MehDB

A prototype/research project that implements Cache-line Aware Exendible Hashing per [this paper.](https://www.usenix.org/system/files/fast19-nam.pdf)

The code for this is quite ugly, but performs well for updating single u64 values. Can be combined with a LSM or similar structure to create a fully-functional, O(1) read/write KV store using at-most 3 disk reads maximum. Performance is *only* good on SSD-devices, but the hypothetical log-structure can exist on higher-capacity spinning rust HDDs as we only will read it for final value retrieval.
