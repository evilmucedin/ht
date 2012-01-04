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
    class TAtomicTraits;

    template <class T>
    class TKeyTraits;

    template <class T>
    class TValueTraits;

    template<class T, size_t N>
    struct TReserved {
        static T Value();
    };

    // reserved values
    // pointers for first double word are always invalid

    template <class T>
    struct TReserved<T*, 0> {
        inline static T* Value() {
            return (T*)0;
        }
    };
    template <class T>
    struct TReserved<T*, 1> {
        inline static T* Value() {
            return (T*)1;
        }
    };
    template <class T>
    struct TReserved<T*, 2> {
        inline static T* Value() {
            return (T*)2;
        }
    };
    template <class T>
    struct TReserved<T*, 3> {
        inline static T* Value() {
            return (T*)3;
        }
    };

    template <class T>
    struct TReserved<const T*, 0> {
        inline static const T* Value() {
            return (const T*)0;
        }
    };
    template <class T>
    struct TReserved<const T*, 1> {
        inline static const T* Value() {
            return (const T*)1;
        }
    };
    template <class T>
    struct TReserved<const T*, 2> {
        inline static const T* Value() {
            return (const T*)2;
        }
    };
    template <class T>
    struct TReserved<const T*, 3> {
        inline static const T* Value() {
            return (const T*)3;
        }
    };

    template <>
    struct TReserved<size_t, 0> {
        inline static size_t Value() {
            return 0x7FFFFFFFFFFFFFFCull;
        }
    };
    template <>
    struct TReserved<size_t, 1> {
        inline static size_t Value() {
            return 0x7FFFFFFFFFFFFFFDull;
        }
    };
    template <>
    struct TReserved<size_t, 2> {
        inline static size_t Value() {
            return 0x7FFFFFFFFFFFFFFEull;
        }
    };
    template <>
    struct TReserved<size_t, 3> {
        inline static size_t Value() {
            return 0x7FFFFFFFFFFFFFFFull;
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
    class TAtomicTraits<T*> : public TAtomicTraitsBase<T*> {
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
    class TAtomicTraits<size_t> : public TAtomicTraitsBase<size_t> {
    public:
        static bool CompareAndSet(TAtomicType& dest, TType newValue, TType oldValue) {
            return ::AtomicCas((Atomic*)&dest, newValue, oldValue);
        }

        static std::string ToString(const TType& t) {
            return ::ToString<size_t>(t);
        }
    };

    template <>
    std::string TAtomicTraits<const char*>::ToString(const TAtomicTraits<const char*>::TType& s);

    template <class T>
    class TKeyTraitsBase : public TAtomicTraits<T> {
    public:
        typedef typename TAtomicTraits<T>::TType TKey;
        typedef typename TAtomicTraits<T>::TAtomicType TAtomicKey;

        inline static T None() {
            return TReserved<T, 0>::Value();
        }

        inline static bool IsReserved(const TKey& k) {
            return k == None();
        }
    };

    template <class T>
    class TKeyTraits<const T*> : public TKeyTraitsBase<const T*> {
    public:
        typedef typename TKeyTraitsBase<const T*>::TKey TKey;
        typedef typename TKeyTraitsBase<const T*>::TAtomicKey TAtomicKey;
    };

    template <>
    class TKeyTraits<const char*> : public TKeyTraitsBase<const char*> {
    public:
    };

    template <>
    class TKeyTraits<size_t> : public TKeyTraitsBase<size_t> {
    public:
    };

    // value specific traits

    template <class T>
    class TValueTraitsBase : public TAtomicTraits<T> {
    public:
        typedef typename TAtomicTraits<T>::TType TValue;
        typedef typename TAtomicTraits<T>::TAtomicType TAtomicValue;

        inline static T None() {
            return TReserved<T, 0>::Value();
        }
        inline static T Baby() {
            return TReserved<T, 1>::Value();
        }
        inline static T Copied() {
            return TReserved<T, 2>::Value();
        }
        inline static T Deleted() {
            return TReserved<T, 3>::Value();
        }

    };

    // depends on canonical address form of 64-bit pointers
    // see http://support.amd.com/us/Embedded_TechDocs/24593.pdf
    // bit 62 not equal to bit 63 means state is COPYING
    template <class T>
    class TValueTraits<const T*> : public TValueTraitsBase<const T*> {
        static const size_t SIGNIFICANT_BITS = 0x0000FFFFFFFFFFFFULL;
    public:
        typedef typename TValueTraitsBase<const T*>::TValue TValue;
        typedef typename TValueTraitsBase<const T*>::TAtomicValue TAtomicValue;

        static TValue PureValue(const TValue& p) {
            size_t& x = (size_t&)p;
            size_t b62 = (x >> 62) & 1;
            if (b62)
                return (TValue)(x | ~(SIGNIFICANT_BITS));
            else
                return (TValue)(x & SIGNIFICANT_BITS);
        }

        static bool IsCopying(const TValue& p) {
            // hope that optimizer will make this code much better
            size_t& x = (size_t&)p;
            size_t b62 = (x >> 62) & 1;
            size_t b63 = (x >> 63) & 1;
            if (b63 != b62)
                return true;
            return false;
        }

        static void SetCopying(TAtomicValue& p) {
            Atomic& x = (Atomic&)p;
            const size_t b62 = (x >> 62) & 1;

            if (!b62)
                AtomicOr(x, 1UL << 63);
            else
                AtomicAnd(x, ~(1UL << 63));
        }

        static bool IsReserved(const TValue& p) {
            return (size_t)p <= (size_t)TReserved<TValue, 3>::Value();
        }
        static bool IsGood(const TValue& p) {
            return ((size_t)p & SIGNIFICANT_BITS) == (size_t)p;
        }
    };

    template <>
    class TValueTraits<size_t> : public TValueTraitsBase<size_t> {
        static const size_t SIGNIFICANT_BITS = 0x7FFFFFFFFFFFFFFFULL;
        static const size_t COPYING_FLAG = ~SIGNIFICANT_BITS;

    public:
        static TValue PureValue(const TValue& x) {
            return x & SIGNIFICANT_BITS;
        }

        static bool IsCopying(const TValue& x) {
            return x & COPYING_FLAG;
        }

        static void SetCopying(TAtomicValue& x) {
            AtomicOr((Atomic&)x, COPYING_FLAG);
        }

        static bool IsReserved(const TValue& x) {
            return x >= TReserved<TValue, 0>::Value();
        }
        static bool IsGood(const TValue& p) {
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

        inline bool operator() (const Key& lft, const Key& rgh) {
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

        inline bool operator()(const Val& lft, const Val& rgh) {
            if (EXPECT_FALSE(TValueTraits<Val>::IsCopying(lft) != TValueTraits<Val>::IsCopying(rgh)))
                return false;
            const Val lftPure = TValueTraits<Val>::PureValue(lft);
            const Val rghPure = TValueTraits<Val>::PureValue(rgh);
            if (EXPECT_FALSE(TValueTraits<Val>::IsReserved(lftPure) || TValueTraits<Val>::IsReserved(rghPure)))
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

        inline size_t operator()(const Key& key) {
            return Hash(key);
        }

    private:
        HashFn Hash;
    };

    template <class T>
    std::string KeyToString(const typename TKeyTraits<T>::TKey& arg) {
        if (arg == TKeyTraits<T>::None())
            return "NONE";
        return TAtomicTraits<T>::ToString(arg);
    }

    template <class T>
    std::string ValueToString(const typename TValueTraits<T>::TValue& arg) {
        typename TValueTraits<T>::TValue argPure = TValueTraits<T>::PureValue(arg);

        std::stringstream tmp;
        if (argPure == TValueTraits<T>::None())
            tmp << "NONE";
        else if (argPure == TValueTraits<T>::Copied())
            tmp << "COPIED";
        else if (argPure == TValueTraits<T>::Baby())
            tmp << "BABY";
        else if (argPure == TValueTraits<T>::Deleted())
            tmp << "DELETED";
        else
            tmp << TAtomicTraits<T>::ToString(argPure);

        if (TValueTraits<T>::IsCopying(arg))
            tmp << "(COPYING)";
        return tmp.str();
    }
}
