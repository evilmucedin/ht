#pragma once

#include "platform_specific.h"

#include <util/system/atomic.h>
#include <util/str_stl.h>
#include <util/digest/numeric.h>
#include <util/string/cast.h>

namespace NLFHT {
    // const T* CAS

    using ::AtomicCas;        

    template <class T>
    static bool AtomicCas(const T* volatile* target, const T* exchange, const T* compare) {
        return ::AtomicCas((TAtomic*)target, (intptr_t)exchange, (intptr_t)compare);
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
    // pointers for first double word are alwys invalid

    template <class T>
    struct TReserved<T*, 0> {
        static T* Value() {
            return (T*)0;
        }
    };
    template <class T>
    struct TReserved<T*, 1> {
        static T* Value() {
            return (T*)1;
        }
    };
    template <class T>
    struct TReserved<T*, 2> {
        static T* Value() {
            return (T*)2;
        }
    };
    template <class T>
    struct TReserved<T*, 3> {
        static T* Value() {
            return (T*)3;
        }
    };

    template <class T>
    struct TReserved<const T*, 0> {
        static const T* Value() {
            return (const T*)0;
        }
    };
    template <class T>
    struct TReserved<const T*, 1> {
        static const T* Value() {
            return (const T*)1;
        }
    };
    template <class T>
    struct TReserved<const T*, 2> {
        static const T* Value() {
            return (const T*)2;
        }
    };
    template <class T>
    struct TReserved<const T*, 3> {
        static const T* Value() {
            return (const T*)3;
        }
    };

    template <>
    struct TReserved<size_t, 0> {
        static size_t Value() {
            return 0ull;
        }
    };
    template <>
    struct TReserved<size_t, 1> {
        static size_t Value() {
            return 0x7FFFFFFFFFFFFFFDull;
        }
    };
    template <>
    struct TReserved<size_t, 2> {
        static size_t Value() {
            return 0x7FFFFFFFFFFFFFFEull;
        }
    };
    template <>
    struct TReserved<size_t, 3> {
        static size_t Value() {
            return 0x7FFFFFFFFFFFFFFFull;
        }
    };

    // base traits of each class, that can be accessed atomically

    template <class T>
    class TAtomicTraitsBase {
    public:
        typedef T TType;
        typedef volatile T TAtomicType;

        void Ref(const TType&) {
        }
        void UnRef(const TType&, size_t) {
        }
    };

    template <class T>
    class TAtomicTraits<T*> : public TAtomicTraitsBase<T*> {
    public:    
        typedef typename TAtomicTraitsBase<T*>::TType TType;
        typedef typename TAtomicTraitsBase<T*>::TAtomicType TAtomicType;

        static bool CompareAndSet(TAtomicType& dest, TType newValue, TType oldValue) {
             return AtomicCas(&dest, newValue, oldValue);
        }

        static Stroka ToString(const TType& t) {
            return ::ToString<size_t>((size_t)t);
        }

        struct TAreEqual {
            bool operator () (const TType& lft, const TType& rgh) {
                return lft == rgh;
            }
        };
    };

    template <> 
    class TAtomicTraits<size_t> : public TAtomicTraitsBase<size_t> {
    public:
        static bool CompareAndSet(TAtomicType& dest, TType newValue, TType oldValue) {
            return ::AtomicCas((TAtomic*)&dest, newValue, oldValue);
        }

        static Stroka ToString(const TType& t) {
            return ::ToString<size_t>(t);
        }

        struct TAreEqual {
            bool operator () (const TType& lft, const TType& rgh) {
                return lft == rgh;
            }
        };
    };

    template <>
    Stroka TAtomicTraits<const char*>::ToString(const TAtomicTraits<const char*>::TType& s);

    template <> 
    class TAtomicTraits<const char*>::TAreEqual {
    private:
        TEqualTo<const char*> EqualTo;
    public:
        bool operator () (const TAtomicTraits<const char*>::TType& lft,
                          const TAtomicTraits<const char*>::TType& rgh) {
            return EqualTo(lft, rgh);
        }
    };

    // key specifil traits

    template <class T>
    class TKeyTraitsBase : public TAtomicTraits<T> {
    public:
        typedef typename TAtomicTraits<T>::TType TKey;
        typedef typename TAtomicTraits<T>::TAtomicType TAtomicKey;

        static T None() {
            return TReserved<T, 0>::Value();
        }
        static T Tombstone() {
            return TReserved<T, 1>::Value();
        }

        void UnRef(const TKey&, size_t) {
        }
    };

    template <class T>
    class TKeyTraits<const T*> : public TKeyTraitsBase<const T*> {
    public:    
        typedef typename TKeyTraitsBase<const T*>::TKey TKey;
        typedef typename TKeyTraitsBase<const T*>::TAtomicKey y;

        class THashFunc {
        public:    
            size_t operator () (const TKey& arg) {
                return NumericHash<const T*>(arg);
            }
        };
    };

    template <>
    class TKeyTraits<const char*> : public TKeyTraitsBase<const char*> {
    public:
        class THashFunc {
        private:
            THash<const char*> Hash;
        public:
            size_t operator () (const TKey& arg) {
                if (arg == None())
                    return (size_t)arg;
                return Hash(arg);
            }
        };
    };

    template <>
    class TKeyTraits<size_t> : public TKeyTraitsBase<size_t> {
    public:
        class THashFunc {
        private:
            THash<size_t> Hash;
        public:
            size_t operator () (const TKey& arg) {
                if (arg == None() || arg == Tombstone())
                    return (size_t)arg;
                return Hash(arg);
            }
        };
    };

    // value specific traits

    template <class T>
    class TValueTraitsBase : public TAtomicTraits<T> {
    public:
        typedef typename TAtomicTraits<T>::TType TValue;
        typedef typename TAtomicTraits<T>::TAtomicType TAtomicValue;

        static T None() {
            return TReserved<T, 0>::Value();
        }
        static T Baby() {
            return TReserved<T, 1>::Value();
        }
        static T Copied() {
            return TReserved<T, 2>::Value();
        }
        static T Deleted() {
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
            size_t& x = (size_t&)p;
            size_t b62 = (x >> 62) & 1;

            if (!b62)
                AtomicOr(x, 1UL << 63);
            else
                AtomicAnd(x, ~(1UL << 63));
        }

        static bool IsReserved(const TValue& p) {
            return (size_t)p < 4;
        }

        void ReadAndRef(TValue& value, const TAtomicValue& atomicValue) {
            value = PureValue((TValue)atomicValue);
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
            AtomicOr(x, COPYING_FLAG);
        } 

        static bool IsReserved(const TValue& x) {
            return x == TReserved<TValue, 0>::Value() || x >= TReserved<TValue, 1>::Value();
        }

        void ReadAndRef(TValue& value, const TAtomicValue& atomicValue) {
            value = PureValue((TValue)atomicValue);
        }
    };

    // work with key and value templates
    // by specifying templates here you can optimize hashtable work

    template <class T> 
    class TKeysAreEqual {
    private:
        typename TAtomicTraits<T>::TAreEqual AreEqual;
    public:
        typedef typename TKeyTraits<T>::TKey TKey;

        bool operator () (const TKey& lft, const TKey& rgh) {
            if (lft == TKeyTraits<T>::None() || rgh == TKeyTraits<T>::None())
                return lft == rgh;
            return AreEqual(lft, rgh);
        }
    };

    template <class T> 
    class TValuesAreEqual {
    private:
        typename TAtomicTraits<T>::TAreEqual AreEqual;
    public:
        typedef typename TValueTraits<T>::TValue TValue;

        bool operator () (const TValue& lft, const TValue& rgh) {
            if (TValueTraits<T>::IsCopying(lft) != TValueTraits<T>::IsCopying(rgh))
                return false;
            TValue lftPure = TValueTraits<T>::PureValue(lft);
            TValue rghPure = TValueTraits<T>::PureValue(rgh);
            if (TValueTraits<T>::IsReserved(lftPure) ||
                TValueTraits<T>::IsReserved(rghPure))
                return lft == rgh;
            return AreEqual(lft, rgh);
        }
    };

    template <class T>
    Stroka KeyToString(const typename TKeyTraits<T>::TKey& arg) {
        TKeysAreEqual<T> areEqual;
        if (areEqual(arg, TKeyTraits<T>::None()))
            return "NONE";
        return TAtomicTraits<T>::ToString(arg);
    }

    template <class T>
    Stroka ValueToString(const typename TValueTraits<T>::TValue& arg) {
        TValuesAreEqual<T> areEqual;
        typename TValueTraits<T>::TValue argPure = TValueTraits<T>::PureValue(arg); 

        TStringStream tmp;
        if (areEqual(argPure, TValueTraits<T>::None()))
            tmp << "NONE";
        else if (areEqual(argPure, TValueTraits<T>::Copied()))
            tmp << "COPIED";
        else if (areEqual(argPure, TValueTraits<T>::Baby()))
            tmp << "BABY";
        else if (areEqual(argPure, TValueTraits<T>::Deleted()))
            tmp << "DELETED";
        else
            tmp << TAtomicTraits<T>::ToString(argPure);

        if (TValueTraits<T>::IsCopying(arg))
            tmp << "(COPYING)";
        return tmp.Str();
    }
}