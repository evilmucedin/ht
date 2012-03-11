#include "atomic.h"

namespace NLFHT {
    // Key and value managers are supposed to be exclusive
    // property of table. They hold very private infomation.
    // Copying then would have too complex semantics, that's why
    // TBaseManager inherits TNonCopyable.
    template <class Prt>
    class BaseManager : NonCopyable
    {
    public:
        typedef Prt TParent;

        BaseManager(TParent* parent)
            : Parent(parent)
        {
        }

        TParent* GetParent()
        {
            return Parent;
        }

        void RegisterThread()
        {
        }
        void ForgetThread()
        {
        }

        // JUST TO DEBUG
        std::string ToString()
        {
            return "";
        }
    private:
        TParent* Parent;
    };

    template <class Prt>
    class DefaultKeyManager : public BaseManager<Prt> {
    public:
        typedef Prt Parent;
        typedef typename Parent::TKey Key;

        DefaultKeyManager(Parent* parent)
            : BaseManager<Prt>(parent)
        {
        }

        Key CloneAndRef(Key k)
        {
            return k;
        }

        void UnRef(Key, size_t = 1)
        {
        }
    };

    template <class Prt>
    class DefaultValueManager : public BaseManager<Prt>
    {
    public:
        typedef Prt TParent;
        typedef typename TParent::TValue Value;

        DefaultValueManager(TParent* parent)
            : BaseManager<Prt>(parent)
        {
        }

        Value CloneAndRef(Value v)
        {
            return v;
        }

        void ReadAndRef(Value& dest, typename ValueTraits<Value>::TAtomicValue const& src) {
            dest = ValueTraits<Value>::PureValue(Value(src));
        }

        void UnRef(Value, size_t = 1) {
        }
    };
}
