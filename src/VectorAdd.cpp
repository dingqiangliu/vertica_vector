/* Copyright (c) DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2024 -*- C++ -*- */
/*
 * Description: User Defined Scalar Function for vector_add
 *
 * Create Date: Dec. 12, 2024
 */

#include "Vertica.h"
#include "Arrays/Accessors.h"

using namespace Vertica;
using namespace std;

class VectorAdd : public ScalarFunction
{
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &argReader,
                              BlockWriter &resWriter)
    {
        do {
            if (argReader.isNull(0) || argReader.isNull(1))
            {
                resWriter.setNull();
            } 
            else
            {
                Array::ArrayWriter outArray = resWriter.getArrayRef(0);
                Array::ArrayReader argArray1  = argReader.getArrayRef(0);
                Array::ArrayReader argArray2  = argReader.getArrayRef(1);

                for (int i = 0; argArray1->hasData() && argArray2->hasData(); i++) {
                    vfloat value1 = argArray1->getFloatRef(0);
                    vfloat value2 = argArray2->getFloatRef(0);
                    if(vfloatIsNull(value1) || vfloatIsNull(value2))
                        outArray->setNull();
                    else
                        outArray->setFloat(value1 + value2);

                    outArray->next();
                    argArray1->next();
                    argArray2->next();
                }

                outArray.commit();
            }

            resWriter.next();
        } while (argReader.next());
     }
};


class VectorAddFactory : public ScalarFunctionFactory
{
    ScalarFunction *createScalarFunction(ServerInterface &interface) override
    {
        return vt_createFuncObject<VectorAdd>(interface.allocator);
    }


    virtual void getPrototype(ServerInterface &srvInterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        ColumnTypes typeFloat;
        typeFloat.addFloat();

        argTypes.addArrayType(typeFloat);
        argTypes.addArrayType(typeFloat);
        returnType.addArrayType(typeFloat);
    }


    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes)
    {
        const VerticaType &inputType = argTypes.getColumnType(0);
        returnTypes.addArg(inputType);
    }


public:
    VectorAddFactory() 
    {
        vol = STABLE;
        strict = STRICT;
    }
};


RegisterFactory(VectorAddFactory);
