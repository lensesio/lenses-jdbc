package kotlin

public open class TypeCastException : ClassCastException {
    constructor()

    constructor(message: String?) : super(message)
}