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
        std::string ToString() {
            return "";
        }
    private:
        TParent* Parent;
    };

    template <class Prt>
    class DefaultKeyManager : public BaseManager<Prt> {
    public:
        typedef Prt TParent;
        typedef typename TParent::TKey TKey;

        DefaultKeyManager(TParent* parent)
            : BaseManager<Prt>(parent)
        {
        }

        TKey CloneAndRef(TKey k) {
            return k;
        }

        void UnRef(TKey, size_t = 1) {
        }
    };

    template <class Prt>
    class DefaultValueManager : public BaseManager<Prt> {
    public:
        typedef Prt TParent;
        typedef typename TParent::TValue TValue;

        DefaultValueManager(TParent* parent)
            : BaseManager<Prt>(parent)
        {
        }

        TValue CloneAndRef(TValue v) {
            return v;
        }

        void ReadAndRef(TValue& dest, typename TValueTraits<TValue>::TAtomicValue const& src) {
            dest = TValueTraits<TValue>::PureValue(TValue(src));
        }

        void UnRef(TValue, size_t = 1) {
        }
    };
}
