#pragma once

#include "atomic.h"

#include <string>

namespace NLFHT {
    // const T* CAS

    using ::AtomicCas;

    template <class T>
    static bool AtomicCas(const T* volatile* target, const T* exchange, const T* compare) {
        return ::AtomicCas((Atomic*)target, (AtomicBase)exchange, (AtomicBase)compare);
    }

    // traits classes declarations

    template <class T>
    class AtomicTraits;

    template <class T>
    class KeyTraits;

    template <class T>
    class ValueTraits;

    template<class T, size_t N>
    struct Reserved {
        static T Value();
    };

    // reserved values
    // pointers for first double word are always invalid

    template <class T>
    struct Reserved<T*, 0> {
        inline static T* Value() {
            return (T*)0;
        }
    };
    template <class T>
    struct Reserved<T*, 1> {
        inline static T* Value() {
            return (T*)1;
        }
    };
    template <class T>
    struct Reserved<T*, 2> {
        inline static T* Value() {
            return (T*)2;
        }
    };
    template <class T>
    struct Reserved<T*, 3> {
        inline static T* Value() {
            return (T*)3;
        }
    };

    template <class T>
    struct Reserved<const T*, 0> {
        inline static const T* Value() {
            return (const T*)0;
        }
    };
    template <class T>
    struct Reserved<const T*, 1> {
        inline static const T* Value() {
            return (const T*)1;
        }
    };
    template <class T>
    struct Reserved<const T*, 2> {
        inline static const T* Value() {
            return (const T*)2;
        }
    };
    template <class T>
    struct Reserved<const T*, 3> {
        inline static const T* Value() {
            return (const T*)3;
        }
    };

    template <>
    struct Reserved<uint64_t, 0> {
        inline static uint64_t Value() {
            return 0x7FFFFFFFFFFFFFFCull;
        }
    };
    template <>
    struct Reserved<uint64_t, 1> {
        inline static uint64_t Value() {
            return 0x7FFFFFFFFFFFFFFDull;
        }
    };
    template <>
    struct Reserved<uint64_t, 2> {
        inline static uint64_t Value() {
            return 0x7FFFFFFFFFFFFFFEull;
        }
    };
    template <>
    struct Reserved<uint64_t, 3> {
        inline static uint64_t Value() {
            return 0x7FFFFFFFFFFFFFFFull;
        }
    };

    template <>
    struct Reserved<uint32_t, 0> {
        inline static uint32_t Value() {
            return 0x7FFFFFFCul;
        }
    };
    template <>
    struct Reserved<uint32_t, 1> {
        inline static uint32_t Value() {
            return 0x7FFFFFFDul;
        }
    };
    template <>
    struct Reserved<uint32_t, 2> {
        inline static uint32_t Value() {
            return 0x7FFFFFFEul;
        }
    };
    template <>
    struct Reserved<uint32_t, 3> {
        inline static uint32_t Value() {
            return 0x7FFFFFFFul;
        }
    };

    // base traits of each class, that can be accessed atomically

    template <class T>
    class TAtomicTraitsBase {
    public:
        typedef T TType;
        typedef volatile T TAtomicType;
    };

    template <class T>
    class AtomicTraits<T*> : public TAtomicTraitsBase<T*> {
    public:
        typedef typename TAtomicTraitsBase<T*>::TType TType;
        typedef typename TAtomicTraitsBase<T*>::TAtomicType TAtomicType;

        static bool CompareAndSet(TAtomicType& dest, TType newValue, TType oldValue) {
             return AtomicCas(&dest, newValue, oldValue);
        }

        static std::string ToString(const TType& t) {
            return ::ToString<size_t>((size_t)t);
        }
    };

    template <>
    class AtomicTraits<size_t> : public TAtomicTraitsBase<size_t> {
    public:
        static bool CompareAndSet(TAtomicType& dest, TType newValue, TType oldValue) {
            return ::AtomicCas((Atomic*)&dest, newValue, oldValue);
        }

        static std::string ToString(const TType& t) {
            return ::ToString<size_t>(t);
        }
    };

    template <>
    std::string AtomicTraits<const char*>::ToString(const AtomicTraits<const char*>::TType& s);

    template <class T>
    class TKeyTraitsBase : public AtomicTraits<T> {
    public:
        typedef typename AtomicTraits<T>::TType TKey;
        typedef typename AtomicTraits<T>::TAtomicType TAtomicKey;

        inline static T None() {
            return Reserved<T, 0>::Value();
        }

        inline static bool IsReserved(TKey k) {
            return k == None();
        }
    };

    template <class T>
    class KeyTraits<const T*> : public TKeyTraitsBase<const T*> {
    public:
        typedef typename TKeyTraitsBase<const T*>::TKey TKey;
        typedef typename TKeyTraitsBase<const T*>::TAtomicKey TAtomicKey;
    };

    template <>
    class KeyTraits<const char*> : public TKeyTraitsBase<const char*> {
    public:
    };

    template <>
    class KeyTraits<size_t> : public TKeyTraitsBase<size_t> {
    public:
    };

    // value specific traits

    template <class T>
    class TValueTraitsBase : public AtomicTraits<T> {
    public:
        typedef typename AtomicTraits<T>::TType TValue;
        typedef typename AtomicTraits<T>::TAtomicType TAtomicValue;

        inline static T None() {
            return Reserved<T, 0>::Value();
        }
        inline static T Baby() {
            return Reserved<T, 1>::Value();
        }
        inline static T Copied() {
            return Reserved<T, 2>::Value();
        }
        inline static T Deleted() {
            return Reserved<T, 3>::Value();
        }

    };

    // depends on canonical address form of 64-bit pointers
    // see http://support.amd.com/us/Embedded_TechDocs/24593.pdf
    // bit 62 not equal to bit 63 means state is COPYING
    template <class T>
    class ValueTraits<const T*> : public TValueTraitsBase<const T*> {
        static const size_t NBITS_SIZE_T = sizeof(size_t)*8;
        static const size_t SIGNIFICANT_BITS = ((size_t)1 << (NBITS_SIZE_T - 2)) - 1;
    public:
        typedef typename TValueTraitsBase<const T*>::TValue TValue;
        typedef typename TValueTraitsBase<const T*>::TAtomicValue TAtomicValue;

        static TValue PureValue(TValue p) {
            size_t& x = (size_t&)p;
            size_t b62 = (x >> (NBITS_SIZE_T - 2)) & 1;
            if (b62)
                return (TValue)(x | ~(SIGNIFICANT_BITS));
            else
                return (TValue)(x & SIGNIFICANT_BITS);
        }

        static bool IsCopying(TValue p) {
            // hope that optimizer will make this code much better
            size_t& x = (size_t&)p;
            size_t b62 = (x >> (NBITS_SIZE_T - 2)) & 1;
            size_t b63 = (x >> (NBITS_SIZE_T - 1)) & 1;
            return (b63 != b62);
        }

        static void SetCopying(TAtomicValue& p) {
            Atomic& x = (Atomic&)p;
            AtomicBase b62 = (x >> (NBITS_SIZE_T - 2)) & 1;

            if (!b62)
                AtomicOr(x, 1UL << (NBITS_SIZE_T - 1));
            else
                AtomicAnd(x, ~(1UL << (NBITS_SIZE_T - 1)));
        }

        static bool IsReserved(TValue p) {
            return (size_t)p <= (size_t)Reserved<TValue, 3>::Value();
        }
        static bool IsGood(TValue p) {
            return ((size_t)p & SIGNIFICANT_BITS) == (size_t)p;
        }
    };

    template <>
    class ValueTraits<size_t> : public TValueTraitsBase<size_t> {
        static const size_t NBITS_SIZE_T = sizeof(size_t)*8;
        static const size_t COPYING_FLAG = ((size_t)1) << (NBITS_SIZE_T - 1);
        static const size_t SIGNIFICANT_BITS = (size_t(-1)) & ~COPYING_FLAG;

    public:
        static TValue PureValue(TValue x) {
            return x & SIGNIFICANT_BITS;
        }

        static bool IsCopying(TValue x) {
            return x & COPYING_FLAG;
        }

        static void SetCopying(TAtomicValue& x) {
            AtomicOr((Atomic&)x, COPYING_FLAG);
        }

        static bool IsReserved(TValue x) {
            return x >= Reserved<TValue, 0>::Value();
        }
        static bool IsGood(TValue p) {
            return (p & SIGNIFICANT_BITS) == p;
        }
    };

    template <class Key, class KeyCmp>
    class TKeysAreEqual {
    public:
        TKeysAreEqual(const KeyCmp& areEqual)
            : AreEqual(areEqual)
        {
        }

        inline bool operator() (const Key& lft, const Key& rgh) const {
            return AreEqual(lft, rgh);
        }

        KeyCmp GetImpl() {
            return AreEqual;
        }
    private:
        KeyCmp AreEqual;
    };

    template <class Val, class ValCmp>
    class TValuesAreEqual {
    public:
        TValuesAreEqual(const ValCmp& areEqual)
            : AreEqual(areEqual)
        {
        }

        FORCED_INLINE bool operator()(const Val& lft, const Val& rgh) {
            if (EXPECT_FALSE(ValueTraits<Val>::IsCopying(lft) != ValueTraits<Val>::IsCopying(rgh)))
                return false;
            const Val lftPure = ValueTraits<Val>::PureValue(lft);
            const Val rghPure = ValueTraits<Val>::PureValue(rgh);
            if (EXPECT_FALSE(ValueTraits<Val>::IsReserved(lftPure) || ValueTraits<Val>::IsReserved(rghPure)))
                return lft == rgh;
            return AreEqual(lft, rgh);
        }

        ValCmp GetImpl() {
            return AreEqual;
        }

    private:
        ValCmp AreEqual;
    };

    template <class Key, class HashFn>
    class THashFunc {
    public:
        THashFunc(const HashFn& hash)
            : Hash(hash)
        {
        }

        inline size_t operator()(const Key& key) const {
            return Hash(key);
        }

    private:
        HashFn Hash;
    };

    template <class T>
    std::string KeyToString(const typename KeyTraits<T>::TKey& arg) {
        if (arg == KeyTraits<T>::None())
            return "NONE";
        return AtomicTraits<T>::ToString(arg);
    }

    template <class T>
    std::string ValueToString(const typename ValueTraits<T>::TValue& arg) {
        typename ValueTraits<T>::TValue argPure = ValueTraits<T>::PureValue(arg);

        std::stringstream tmp;
        if (argPure == ValueTraits<T>::None())
            tmp << "NONE";
        else if (argPure == ValueTraits<T>::Copied())
            tmp << "COPIED";
        else if (argPure == ValueTraits<T>::Baby())
            tmp << "BABY";
        else if (argPure == ValueTraits<T>::Deleted())
            tmp << "DELETED";
        else
            tmp << AtomicTraits<T>::ToString(argPure);

        if (ValueTraits<T>::IsCopying(arg))
            tmp << "(COPYING)";
        return tmp.str();
    }
}
