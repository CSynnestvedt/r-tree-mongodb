/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=8 sts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/*
 * A counting Bloom filter implementation.  This allows consumers to
 * do fast probabilistic "is item X in set Y?" testing which will
 * never answer "no" when the correct answer is "yes" (but might
 * incorrectly answer "yes" when the correct answer is "no").
 */

#ifndef mozilla_BloomFilter_h
#define mozilla_BloomFilter_h

#include "mozilla/Assertions.h"
#include "mozilla/Likely.h"

#include <stdint.h>
#include <string.h>

namespace mozilla {

/*
 * This class implements a classic Bloom filter as described at
 * <http://en.wikipedia.org/wiki/Bloom_filter>.  This allows quick
 * probabilistic answers to the question "is object X in set Y?" where the
 * contents of Y might not be time-invariant.  The probabilistic nature of the
 * test means that sometimes the answer will be "yes" when it should be "no".
 * If the answer is "no", then X is guaranteed not to be in Y.
 *
 * The filter is parametrized on KeySize, which is the size of the key
 * generated by each of hash functions used by the filter, in bits,
 * and the type of object T being added and removed.  T must implement
 * a |uint32_t hash() const| method which returns a uint32_t hash key
 * that will be used to generate the two separate hash functions for
 * the Bloom filter.  This hash key MUST be well-distributed for good
 * results!  KeySize is not allowed to be larger than 16.
 *
 * The filter uses exactly 2**KeySize bit (2**(KeySize-3) bytes) of memory.
 * From now on we will refer to the memory used by the filter as M.
 *
 * The expected rate of incorrect "yes" answers depends on M and on
 * the number N of objects in set Y.  As long as N is small compared
 * to M, the rate of such answers is expected to be approximately
 * 4*(N/M)**2 for this filter.  In practice, if Y has a few hundred
 * elements then using a KeySize of 12 gives a reasonably low
 * incorrect answer rate.  A KeySize of 12 has the additional benefit
 * of using exactly one page for the filter in typical hardware
 * configurations.
 */
template <unsigned KeySize, class T>
class BitBloomFilter {
  /*
   * A counting Bloom filter with 8-bit counters.  For now we assume
   * that having two hash functions is enough, but we may revisit that
   * decision later.
   *
   * The filter uses an array with 2**KeySize entries.
   *
   * Assuming a well-distributed hash function, a Bloom filter with
   * array size M containing N elements and
   * using k hash function has expected false positive rate exactly
   *
   * $  (1 - (1 - 1/M)^{kN})^k  $
   *
   * because each array slot has a
   *
   * $  (1 - 1/M)^{kN}  $
   *
   * chance of being 0, and the expected false positive rate is the
   * probability that all of the k hash functions will hit a nonzero
   * slot.
   *
   * For reasonable assumptions (M large, kN large, which should both
   * hold if we're worried about false positives) about M and kN this
   * becomes approximately
   *
   * $$  (1 - \exp(-kN/M))^k   $$
   *
   * For our special case of k == 2, that's $(1 - \exp(-2N/M))^2$,
   * or in other words
   *
   * $$    N/M = -0.5 * \ln(1 - \sqrt(r))   $$
   *
   * where r is the false positive rate.  This can be used to compute
   * the desired KeySize for a given load N and false positive rate r.
   *
   * If N/M is assumed small, then the false positive rate can
   * further be approximated as 4*N^2/M^2.  So increasing KeySize by
   * 1, which doubles M, reduces the false positive rate by about a
   * factor of 4, and a false positive rate of 1% corresponds to
   * about M/N == 20.
   *
   * What this means in practice is that for a few hundred keys using a
   * KeySize of 12 gives false positive rates on the order of 0.25-4%.
   *
   * Similarly, using a KeySize of 10 would lead to a 4% false
   * positive rate for N == 100 and to quite bad false positive
   * rates for larger N.
   */
 public:
  BitBloomFilter() {
    static_assert(KeySize >= 3, "KeySize too small");
    static_assert(KeySize <= kKeyShift, "KeySize too big");

    // XXX: Should we have a custom operator new using calloc instead and
    // require that we're allocated via the operator?
    clear();
  }

  /*
   * Clear the filter.  This should be done before reusing it.
   */
  void clear();

  /*
   * Add an item to the filter.
   */
  void add(const T* aValue);

  /*
   * Check whether the filter might contain an item.  This can
   * sometimes return true even if the item is not in the filter,
   * but will never return false for items that are actually in the
   * filter.
   */
  bool mightContain(const T* aValue) const;

  /*
   * Methods for add/contain when we already have a hash computed
   */
  void add(uint32_t aHash);
  bool mightContain(uint32_t aHash) const;

 private:
  static const size_t kArraySize = (1 << (KeySize - 3));
  static const uint32_t kKeyMask = (1 << (KeySize - 3)) - 1;
  static const uint32_t kKeyShift = 16;

  static uint32_t hash1(uint32_t aHash) { return aHash & kKeyMask; }
  static uint32_t hash2(uint32_t aHash) {
    return (aHash >> kKeyShift) & kKeyMask;
  }

  bool getSlot(uint32_t aHash) const {
    uint32_t index = aHash / 8;
    uint8_t shift = aHash % 8;
    uint8_t mask = 1 << shift;
    return !!(mCounters[index] & mask);
  }

  void setSlot(uint32_t aHash) {
    uint32_t index = aHash / 8;
    uint8_t shift = aHash % 8;
    uint8_t bit = 1 << shift;
    mCounters[index] |= bit;
  }

  bool getFirstSlot(uint32_t aHash) const { return getSlot(hash1(aHash)); }
  bool getSecondSlot(uint32_t aHash) const { return getSlot(hash2(aHash)); }

  void setFirstSlot(uint32_t aHash) { setSlot(hash1(aHash)); }
  void setSecondSlot(uint32_t aHash) { setSlot(hash2(aHash)); }

  uint8_t mCounters[kArraySize];
};

template <unsigned KeySize, class T>
inline void BitBloomFilter<KeySize, T>::clear() {
  memset(mCounters, 0, kArraySize);
}

template <unsigned KeySize, class T>
inline void BitBloomFilter<KeySize, T>::add(uint32_t aHash) {
  setFirstSlot(aHash);
  setSecondSlot(aHash);
}

template <unsigned KeySize, class T>
MOZ_ALWAYS_INLINE void BitBloomFilter<KeySize, T>::add(const T* aValue) {
  uint32_t hash = aValue->hash();
  return add(hash);
}

template <unsigned KeySize, class T>
MOZ_ALWAYS_INLINE bool BitBloomFilter<KeySize, T>::mightContain(
    uint32_t aHash) const {
  // Check that all the slots for this hash contain something
  return getFirstSlot(aHash) && getSecondSlot(aHash);
}

template <unsigned KeySize, class T>
MOZ_ALWAYS_INLINE bool BitBloomFilter<KeySize, T>::mightContain(
    const T* aValue) const {
  uint32_t hash = aValue->hash();
  return mightContain(hash);
}

/*
 * This class implements a counting Bloom filter as described at
 * <http://en.wikipedia.org/wiki/Bloom_filter#Counting_filters>, with
 * 8-bit counters.
 *
 * Compared to `BitBloomFilter`, this class supports 'remove' operation.
 *
 * The filter uses exactly 2**KeySize bytes of memory.
 *
 * Other characteristics are the same as BitBloomFilter.
 */
template <unsigned KeySize, class T>
class CountingBloomFilter {
 public:
  CountingBloomFilter() {
    static_assert(KeySize <= kKeyShift, "KeySize too big");

    clear();
  }

  /*
   * Clear the filter.  This should be done before reusing it, because
   * just removing all items doesn't clear counters that hit the upper
   * bound.
   */
  void clear();

  /*
   * Add an item to the filter.
   */
  void add(const T* aValue);

  /*
   * Remove an item from the filter.
   */
  void remove(const T* aValue);

  /*
   * Check whether the filter might contain an item.  This can
   * sometimes return true even if the item is not in the filter,
   * but will never return false for items that are actually in the
   * filter.
   */
  bool mightContain(const T* aValue) const;

  /*
   * Methods for add/remove/contain when we already have a hash computed
   */
  void add(uint32_t aHash);
  void remove(uint32_t aHash);
  bool mightContain(uint32_t aHash) const;

 private:
  static const size_t kArraySize = (1 << KeySize);
  static const uint32_t kKeyMask = (1 << KeySize) - 1;
  static const uint32_t kKeyShift = 16;

  static uint32_t hash1(uint32_t aHash) { return aHash & kKeyMask; }
  static uint32_t hash2(uint32_t aHash) {
    return (aHash >> kKeyShift) & kKeyMask;
  }

  uint8_t& firstSlot(uint32_t aHash) { return mCounters[hash1(aHash)]; }
  uint8_t& secondSlot(uint32_t aHash) { return mCounters[hash2(aHash)]; }

  const uint8_t& firstSlot(uint32_t aHash) const {
    return mCounters[hash1(aHash)];
  }
  const uint8_t& secondSlot(uint32_t aHash) const {
    return mCounters[hash2(aHash)];
  }

  static bool full(const uint8_t& aSlot) { return aSlot == UINT8_MAX; }

  uint8_t mCounters[kArraySize];
};

template <unsigned KeySize, class T>
inline void CountingBloomFilter<KeySize, T>::clear() {
  memset(mCounters, 0, kArraySize);
}

template <unsigned KeySize, class T>
inline void CountingBloomFilter<KeySize, T>::add(uint32_t aHash) {
  uint8_t& slot1 = firstSlot(aHash);
  if (MOZ_LIKELY(!full(slot1))) {
    ++slot1;
  }
  uint8_t& slot2 = secondSlot(aHash);
  if (MOZ_LIKELY(!full(slot2))) {
    ++slot2;
  }
}

template <unsigned KeySize, class T>
MOZ_ALWAYS_INLINE void CountingBloomFilter<KeySize, T>::add(const T* aValue) {
  uint32_t hash = aValue->hash();
  return add(hash);
}

template <unsigned KeySize, class T>
inline void CountingBloomFilter<KeySize, T>::remove(uint32_t aHash) {
  // If the slots are full, we don't know whether we bumped them to be
  // there when we added or not, so just leave them full.
  uint8_t& slot1 = firstSlot(aHash);
  if (MOZ_LIKELY(!full(slot1))) {
    --slot1;
  }
  uint8_t& slot2 = secondSlot(aHash);
  if (MOZ_LIKELY(!full(slot2))) {
    --slot2;
  }
}

template <unsigned KeySize, class T>
MOZ_ALWAYS_INLINE void CountingBloomFilter<KeySize, T>::remove(
    const T* aValue) {
  uint32_t hash = aValue->hash();
  remove(hash);
}

template <unsigned KeySize, class T>
MOZ_ALWAYS_INLINE bool CountingBloomFilter<KeySize, T>::mightContain(
    uint32_t aHash) const {
  // Check that all the slots for this hash contain something
  return firstSlot(aHash) && secondSlot(aHash);
}

template <unsigned KeySize, class T>
MOZ_ALWAYS_INLINE bool CountingBloomFilter<KeySize, T>::mightContain(
    const T* aValue) const {
  uint32_t hash = aValue->hash();
  return mightContain(hash);
}

}  // namespace mozilla

#endif /* mozilla_BloomFilter_h */
