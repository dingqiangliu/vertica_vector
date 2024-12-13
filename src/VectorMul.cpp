/* Copyright (c) DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2024 -*- C++ -*- */
/*
 * Description: User Defined Scalar Function for vector_mul
 *
 * Create Date: Dec. 12, 2024
 */

#include "Vertica.h"
#include "Arrays/Accessors.h"

using namespace Vertica;
using namespace std;

class VectorMul : public ScalarFunction
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
                Array::ArrayReader argArray  = argReader.getArrayRef(0);
                const vfloat lamda = argReader.getFloatRef(1);

                Array::ArrayWriter outArray = resWriter.getArrayRef(0);
                for (int i = 0; argArray->hasData(); i++) {
                    vfloat value = argArray->getFloatRef(0);
                    if(vfloatIsNull(value))
                        outArray->setNull();
                    else
                        outArray->setFloat(value * lamda);

                    outArray->next();
                    argArray->next();
                }

                outArray.commit();
            }

            resWriter.next();
        } while (argReader.next());
     }
};


class VectorMulFactory : public ScalarFunctionFactory
{
    ScalarFunction *createScalarFunction(ServerInterface &interface) override
    {
        return vt_createFuncObject<VectorMul>(interface.allocator);
    }


    virtual void getPrototype(ServerInterface &srvInterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        ColumnTypes typeFloat;
        typeFloat.addFloat();

        argTypes.addArrayType(typeFloat);
        argTypes.addFloat();
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
    VectorMulFactory() 
    {
        vol = STABLE;
        strict = STRICT;
    }
};


RegisterFactory(VectorMulFactory);
